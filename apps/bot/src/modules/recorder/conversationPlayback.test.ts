import { OpusEncoder } from '@discordjs/opus';
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { appendAuthoritativeBotikPlaybackPacket, createAuthoritativeBotikPlaybackTrack } from './authoritativePlaybackTrack';
import {
  CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS,
  CRAIG_PLAYBACK_MONO_FRAME_BYTES,
  CraigPlaybackArbiter,
  CraigPlaybackController,
  CraigPlaybackEvent,
  CraigPlaybackOpusEncoder,
  CraigPlaybackTimer,
  CraigPlaybackVoiceConnection,
  duplicateMonoPcmFrameToStereo
} from './conversationPlayback';
import {
  type PlaybackReliabilityIdentity,
  type PlaybackReliabilitySnapshot,
  type PlaybackReliabilityStore,
  type PlaybackReliabilityTerminal
} from './conversationPlaybackReliability';

const recordingId = 'recording-1';
const turnId = 'turn-1';
const attemptId = 'attempt-1';

class MemoryPlaybackReliabilityStore implements PlaybackReliabilityStore {
  private readonly snapshots = new Map<string, Exclude<PlaybackReliabilitySnapshot, { status: 'fresh' }>>();

  inspect(identity: Readonly<PlaybackReliabilityIdentity>): PlaybackReliabilitySnapshot {
    return this.snapshots.get(this.key(identity)) ?? { status: 'fresh' };
  }

  reserve(identity: Readonly<PlaybackReliabilityIdentity>, notAfterUnixMs: number | undefined, reservedAtMs: number) {
    const existing = this.inspect(identity);
    if (existing.status !== 'fresh') return { created: false, snapshot: existing };
    const snapshot = {
      status: 'dispatching' as const,
      reservedAtMs,
      ...(notAfterUnixMs === undefined ? {} : { notAfterUnixMs })
    };
    this.snapshots.set(this.key(identity), snapshot);
    return { created: true, snapshot };
  }

  authorizeFirstPacket(identity: Readonly<PlaybackReliabilityIdentity>, nowMs: number): boolean {
    const snapshot = this.inspect(identity);
    return snapshot.status === 'dispatching' && (snapshot.notAfterUnixMs === undefined || nowMs < snapshot.notAfterUnixMs);
  }

  markStarted(identity: Readonly<PlaybackReliabilityIdentity>, startedAtMs: number): number {
    const current = this.inspect(identity);
    if (current.status === 'started') return current.startedAtMs;
    if (current.status === 'terminal' && current.startedAtMs !== undefined) return current.startedAtMs;
    if (current.status !== 'dispatching') throw new Error('not reserved');
    this.snapshots.set(this.key(identity), { ...current, status: 'started', startedAtMs });
    return startedAtMs;
  }

  markTerminal(identity: Readonly<PlaybackReliabilityIdentity>, terminal: Readonly<PlaybackReliabilityTerminal>): PlaybackReliabilitySnapshot {
    const current = this.inspect(identity);
    if (current.status === 'terminal') return current;
    const snapshot = {
      status: 'terminal' as const,
      reservedAtMs: current.status === 'fresh' ? 0 : current.reservedAtMs,
      ...(current.status === 'fresh' || current.notAfterUnixMs === undefined ? {} : { notAfterUnixMs: current.notAfterUnixMs }),
      ...(current.status === 'started' ? { startedAtMs: current.startedAtMs } : {}),
      terminal: { ...terminal }
    };
    this.snapshots.set(this.key(identity), snapshot);
    return snapshot;
  }

  private key(identity: Readonly<PlaybackReliabilityIdentity>): string {
    return `${identity.recordingId}/${identity.turnId}/${identity.attemptId}`;
  }
}

class FakeEncoder implements CraigPlaybackOpusEncoder {
  readonly frames: Buffer[] = [];

  encode(frame: Buffer): Buffer {
    this.frames.push(Buffer.from(frame));
    return Buffer.from([this.frames.length]);
  }
}

class FakePlaybackTimer implements CraigPlaybackTimer {
  now = 4_000;
  private readonly timers: Array<{ at: number; callback: () => void; cancelled: boolean }> = [];

  schedule(callback: () => void, delayMs: number): unknown {
    const timer = { at: this.now + delayMs, callback, cancelled: false };
    this.timers.push(timer);
    return timer;
  }

  cancel(handle: unknown): void {
    (handle as { cancelled: boolean }).cancelled = true;
  }

  advance(delayMs: number): void {
    const target = this.now + delayMs;
    for (;;) {
      const next = this.timers
        .filter((timer) => !timer.cancelled && timer.at <= target)
        .sort((left, right) => left.at - right.at)
        .at(0);
      if (!next) break;

      this.timers.splice(this.timers.indexOf(next), 1);
      this.now = next.at;
      next.callback();
    }
    this.now = target;
  }
}

class FakeVoiceConnection implements CraigPlaybackVoiceConnection {
  readonly plays: Array<{
    input: string;
    options: { format: string };
  }> = [];
  readonly packets: Buffer[] = [];
  readonly order: string[] = [];
  readonly speaking: boolean[] = [];
  ready = true;
  udpSocket: unknown | null = {};
  stopCalls = 0;
  sendError: Error | undefined;

  play(input: string, options: { format: string }): void {
    this.plays.push({ input, options });
  }

  stopPlaying(): void {
    this.stopCalls++;
  }

  sendAudioFrame(packet: Buffer): void {
    if (this.sendError) throw this.sendError;
    this.packets.push(Buffer.from(packet));
    this.order.push('packet');
  }

  setSpeaking(value: boolean): void {
    this.speaking.push(value);
  }
}

function createFixture(
  hooks?:
    | ((packet: Buffer) => void)
    | {
        onCancellation(cancellation: Readonly<{
          recordingId: string;
          turnId: string;
          attemptId: string;
          cancellationObservedAt?: string;
          cancellationObservedAtMs?: number;
        }>): boolean;
        isAttemptRevoked?(identity: Readonly<{ turnId: string; attemptId: string }>): boolean;
        onPostCancellationPacket?(identity: Readonly<{ turnId: string; attemptId: string }>): boolean;
        reliabilityStore?: PlaybackReliabilityStore;
      }
) {
  const connection = new FakeVoiceConnection();
  const encoder = new FakeEncoder();
  const events: CraigPlaybackEvent[] = [];
  const dispatchedPackets: Buffer[] = [];
  const timer = new FakePlaybackTimer();
  const arbiter = new CraigPlaybackArbiter(() => connection);
  const controller = new CraigPlaybackController({
    recordingId,
    arbiter,
    createOpusEncoder: () => encoder,
    now: () => timer.now,
    timer,
    onPacketDispatched: (packet) => {
      dispatchedPackets.push(Buffer.from(packet));
      connection.order.push('authoritative');
      if (typeof hooks === 'function') hooks(packet);
    },
    onCancellation: typeof hooks === 'object' ? hooks.onCancellation : () => true,
    isAttemptRevoked: typeof hooks === 'object' ? hooks.isAttemptRevoked ?? (() => false) : () => false,
    onPostCancellationPacket: typeof hooks === 'object' ? hooks.onPostCancellationPacket ?? (() => true) : () => true,
    reliabilityStore: typeof hooks === 'object' ? hooks.reliabilityStore ?? new MemoryPlaybackReliabilityStore() : new MemoryPlaybackReliabilityStore(),
    onEvent: (event) => {
      events.push(event);
      connection.order.push(event.type);
    }
  });
  return { arbiter, connection, controller, dispatchedPackets, encoder, events, timer };
}

function start(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    schemaVersion: 1,
    recordingId,
    turnId,
    attemptId,
    type: 'playback-start',
    format: 'pcm_s16le',
    sampleRateHz: 48_000,
    channels: 1,
    ...overrides
  };
}

function audio(sequence: number, pcm: Buffer, overrides: Partial<Record<string, unknown>> = {}) {
  return {
    schemaVersion: 1,
    recordingId,
    turnId,
    attemptId,
    type: 'audio-chunk',
    sequence,
    pcmBase64: pcm.toString('base64'),
    ...overrides
  };
}

function finish(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    schemaVersion: 1,
    recordingId,
    turnId,
    attemptId,
    type: 'playback-finish',
    ...overrides
  };
}

function cancel(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    schemaVersion: 1,
    recordingId,
    turnId,
    attemptId,
    type: 'playback-cancel',
    reason: 'barge-in',
    ...overrides
  };
}

test('requires every durable cancellation port at construction', () => {
  assert.throws(
    // @ts-expect-error durable cancellation is a required controller port
    () => new CraigPlaybackController({
      recordingId,
      arbiter: new CraigPlaybackArbiter(() => new FakeVoiceConnection()),
      onEvent() {}
    }),
    /restart lookup, post-fence, and reliability handlers are required/
  );
});

test('v1 accepts exactly the original five cancellation reasons', () => {
  for (const reason of ['barge-in', 'meeting-ended', 'playback-failed', 'runtime-shutdown', 'superseded']) {
    const fixture = createFixture();
    assert.equal(fixture.controller.handleCommand(cancel({ reason })), true);
  }
  const fixture = createFixture();
  assert.equal(fixture.controller.handleCommand(cancel({ reason: 'arbitrary-reason' })), false);
});

test('a rejected durable cancellation never creates a volatile revocation', () => {
  let durable = false;
  const fixture = createFixture({
    onCancellation: () => durable,
    isAttemptRevoked: () => false
  });
  assert.equal(fixture.controller.handleCommand(cancel()), false);
  assert.equal(fixture.controller.handleCommand(start()), true);
  assert.equal(fixture.controller.handleCommand(audio(0, frame(1))), true);
  assert.deepEqual(fixture.connection.packets, [Buffer.from([1])]);
  durable = true;
  assert.equal(fixture.controller.handleCommand(cancel()), true);
});

function frame(firstSample: number, secondSample = firstSample): Buffer {
  const pcm = Buffer.alloc(CRAIG_PLAYBACK_MONO_FRAME_BYTES);
  pcm.writeInt16LE(firstSample, 0);
  pcm.writeInt16LE(secondSample, 2);
  return pcm;
}

test('dispatches and records only after the direct Discord send accepts each packet', () => {
  const { connection, controller, dispatchedPackets, encoder, events, timer } = createFixture();
  const pcm = frame(-123, 456);

  assert.equal(controller.handleCommand(start()), true);
  assert.equal(controller.handleCommand(audio(0, Buffer.concat([pcm, frame(20)]))), true);
  assert.equal(connection.plays.length, 0);
  assert.deepEqual(encoder.frames[0]?.subarray(0, 8), Buffer.from([0x85, 0xff, 0x85, 0xff, 0xc8, 0x01, 0xc8, 0x01]));
  assert.deepEqual(connection.packets, [Buffer.from([1])]);
  assert.deepEqual(dispatchedPackets, [Buffer.from([1])]);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-started']
  );
  assert.deepEqual(connection.order, ['packet', 'authoritative', 'playback-started']);
  assert.equal((events[0] as Extract<CraigPlaybackEvent, { type: 'playback-started' }>).startedAtMs, 4_000);

  timer.advance(19);
  assert.deepEqual(dispatchedPackets, [Buffer.from([1])]);
  timer.advance(1);
  assert.deepEqual(connection.packets, [Buffer.from([1]), Buffer.from([2])]);
  assert.deepEqual(dispatchedPackets, [Buffer.from([1]), Buffer.from([2])]);
});

test('suppresses playback when its absolute deadline passes before the first Discord packet', () => {
  const fixture = createFixture();
  assert.equal(fixture.controller.handleCommand(start({ notAfterUnixMs: 4_010 })), true);
  fixture.timer.now = 4_010;
  assert.equal(fixture.controller.handleCommand(audio(0, frame(1))), true);

  assert.deepEqual(fixture.connection.packets, []);
  assert.deepEqual(fixture.dispatchedPackets, []);
  assert.deepEqual(fixture.events, [{
    schemaVersion: 1,
    type: 'playback-failed',
    recordingId,
    turnId,
    attemptId,
    code: 'playback-error',
    safeMessage: 'Playback suppressed at its absolute not-after deadline.',
    retryable: false
  }]);
});

test('replays the original started receipt after reconnect without replaying Discord audio', () => {
  const reliabilityStore = new MemoryPlaybackReliabilityStore();
  const original = createFixture({
    onCancellation: () => true,
    reliabilityStore
  });
  original.controller.handleCommand(start());
  original.controller.handleCommand(audio(0, frame(1)));
  original.controller.transportDisconnected();

  const recovered = createFixture({
    onCancellation: () => true,
    reliabilityStore
  });
  recovered.timer.now = 9_000;
  assert.equal(recovered.controller.handleCommand(start()), true);
  assert.equal(recovered.controller.handleCommand(audio(0, frame(1))), true);
  assert.equal(recovered.controller.handleCommand(finish()), true);

  assert.deepEqual(recovered.connection.packets, []);
  assert.equal((recovered.events[0] as Extract<CraigPlaybackEvent, { type: 'playback-started' }>).startedAtMs, 4_000);
  assert.deepEqual(recovered.events.map(({ type }) => type), ['playback-started', 'playback-finished']);
});

test('fails a crash-ambiguous reserved attempt closed without sending audio', () => {
  const reliabilityStore = new MemoryPlaybackReliabilityStore();
  reliabilityStore.reserve({ recordingId, turnId, attemptId }, undefined, 3_900);
  const recovered = createFixture({
    onCancellation: () => true,
    reliabilityStore
  });

  assert.equal(recovered.controller.handleCommand(start()), true);
  assert.equal(recovered.controller.handleCommand(audio(0, frame(1))), true);
  assert.deepEqual(recovered.connection.packets, []);
  assert.deepEqual(recovered.events.map(({ type }) => type), ['playback-failed']);
  assert.equal((recovered.events[0] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).retryable, false);
});

test('deduplicates identical audio commands and rejects a conflicting command id', () => {
  const fixture = createFixture();
  fixture.controller.handleCommand(start());
  fixture.controller.handleCommand(audio(0, frame(1)));
  fixture.timer.now = 8_000;
  fixture.controller.handleCommand(start());
  fixture.controller.handleCommand(audio(0, frame(1)));
  assert.deepEqual(fixture.connection.packets, [Buffer.from([1])]);
  assert.equal((fixture.events[1] as Extract<CraigPlaybackEvent, { type: 'playback-started' }>).startedAtMs, 4_000);

  fixture.controller.handleCommand(audio(0, frame(2)));
  assert.deepEqual(fixture.connection.packets, [Buffer.from([1])]);
  assert.deepEqual(fixture.events.map(({ type }) => type), ['playback-started', 'playback-started', 'playback-failed']);
  assert.equal((fixture.events[2] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).retryable, false);
});

test('encodes full frames in sequence and drops a finish-time partial tail', () => {
  const { connection, controller, encoder, events, timer } = createFixture();
  const tail = Buffer.from([1, 2, 3, 4]);

  controller.handleCommand(start());
  controller.handleCommand(audio(0, Buffer.concat([frame(10), tail])));
  controller.handleCommand(audio(1, frame(20)));
  controller.handleCommand(finish());
  timer.advance(20);

  assert.equal(encoder.frames.length, 2);
  assert.deepEqual(connection.packets, [Buffer.from([1]), Buffer.from([2])]);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-started', 'playback-finished']
  );
});

test('fails the active turn for out-of-order or oversized audio data', () => {
  const invalidFirstSequence = createFixture();
  invalidFirstSequence.controller.handleCommand(start());
  assert.equal(invalidFirstSequence.controller.handleCommand(audio(1, frame(1))), true);
  assert.equal((invalidFirstSequence.events[0] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).code, 'invalid-audio');

  const outOfOrder = createFixture();
  outOfOrder.controller.handleCommand(start());
  assert.equal(outOfOrder.controller.handleCommand(audio(0, frame(1))), true);
  assert.equal(outOfOrder.controller.handleCommand(audio(2, frame(2))), true);
  assert.deepEqual(
    outOfOrder.events.map((event) => event.type),
    ['playback-started', 'playback-failed']
  );
  assert.equal((outOfOrder.events[1] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).code, 'invalid-audio');

  const oversized = createFixture();
  oversized.controller.handleCommand(start());
  assert.equal(oversized.controller.handleCommand(audio(0, Buffer.alloc(19_202))), false);
  assert.deepEqual(
    oversized.events.map(({ type }) => type),
    ['playback-failed']
  );
  assert.equal((oversized.events[0] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).code, 'invalid-audio');
});

test('fails closed once audio exceeds the two second direct-send buffer limit', () => {
  const { connection, controller, dispatchedPackets, events } = createFixture();
  const twoHundredMilliseconds = Buffer.alloc(19_200);

  controller.handleCommand(start());
  for (let sequence = 0; sequence < 10; sequence++) assert.equal(controller.handleCommand(audio(sequence, twoHundredMilliseconds)), true);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-started']
  );

  assert.equal(controller.handleCommand(audio(10, twoHundredMilliseconds)), true);
  assert.equal(connection.stopCalls, 0);
  assert.deepEqual(dispatchedPackets, [Buffer.from([1])]);
  assert.equal((events[1] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).code, 'backpressure');
});

test('cancellation records only the packet already accepted by the direct sender', () => {
  const { connection, controller, dispatchedPackets, events, timer } = createFixture();

  controller.handleCommand(start());
  controller.handleCommand(audio(0, Buffer.concat([frame(1), frame(2)])));
  controller.handleCommand(cancel());
  controller.handleCommand(audio(1, frame(3)));
  controller.handleCommand(cancel());
  timer.advance(1_000);

  assert.equal(connection.stopCalls, 0);
  assert.deepEqual(connection.packets, [Buffer.from([1])]);
  assert.deepEqual(dispatchedPackets, [Buffer.from([1])]);
  assert.deepEqual(connection.speaking, [true, false]);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-started', 'playback-finished']
  );
});

test('persists the trusted cancellation fence before revoking the exact playback attempt', () => {
  const cancellations: unknown[] = [];
  const latePackets: unknown[] = [];
  const revoked = new Set<string>();
  const fixture = createFixture({
    onCancellation: (cancellation) => {
      cancellations.push(cancellation);
      revoked.add(`${cancellation.turnId}/${cancellation.attemptId}`);
      return true;
    },
    isAttemptRevoked: (identity) => revoked.has(`${identity.turnId}/${identity.attemptId}`),
    onPostCancellationPacket: (identity) => { latePackets.push(identity); return true; }
  });
  assert.equal(fixture.controller.handleCommand(start()), true);
  assert.equal(
    fixture.controller.handleCommand({
      schemaVersion: 2,
      meetingId: 'meeting-1',
      recordingId,
      turnId,
      attemptId,
      type: 'playback-cancel',
      reason: 'barge-in',
      cancellationObservedAtMs: 1_776_124_803_000
    }),
    true
  );
  assert.deepEqual(cancellations, [{
    schemaVersion: 2, type: 'playback-cancel', meetingId: 'meeting-1', recordingId, turnId, attemptId,
    cancellationObservedAtMs: 1_776_124_803_000, reason: 'barge-in'
  }]);
  assert.equal(fixture.controller.handleCommand(audio(0, frame(1))), true);
  assert.deepEqual(latePackets, [{ recordingId, turnId, attemptId }]);
  assert.equal(fixture.controller.handleCommand(start({ turnId: 'turn-2', attemptId: 'attempt-2' })), true);
  assert.equal(fixture.controller.handleCommand(audio(0, frame(2), { turnId: 'turn-2', attemptId: 'attempt-2' })), true);
  assert.deepEqual(fixture.connection.packets, [Buffer.from([1])], 'a new attempt generation remains admissible');
});

test('durably revokes cancellation before playback starts', () => {
  const revoked = new Set<string>();
  const fixture = createFixture({
    onCancellation: (cancellation) => { revoked.add(`${cancellation.turnId}/${cancellation.attemptId}`); return true; },
    isAttemptRevoked: (identity) => revoked.has(`${identity.turnId}/${identity.attemptId}`)
  });
  assert.equal(fixture.controller.handleCommand({
    schemaVersion: 2, type: 'playback-cancel', meetingId: 'meeting-1', recordingId, turnId, attemptId,
    cancellationObservedAtMs: 1_776_124_803_000, reason: 'barge-in'
  }), true);
  assert.equal(fixture.controller.handleCommand(start()), true);
  assert.equal(fixture.controller.handleCommand(audio(0, frame(1))), true);
  assert.deepEqual(fixture.connection.packets, []);
});

test('durably revokes an unrelated identity while another identity is active', () => {
  const cancelled: string[] = [];
  const fixture = createFixture({ onCancellation: (cancellation) => {
    cancelled.push(`${cancellation.turnId}/${cancellation.attemptId}`);
    return true;
  } });
  fixture.controller.handleCommand(start({ turnId: 'turn-active', attemptId: 'attempt-active' }));
  assert.equal(fixture.controller.handleCommand({
    schemaVersion: 2, type: 'playback-cancel', meetingId: 'meeting-1', recordingId, turnId, attemptId,
    cancellationObservedAtMs: 1_776_124_803_000, reason: 'barge-in'
  }), true);
  assert.deepEqual(cancelled, [`${turnId}/${attemptId}`]);
  assert.equal(fixture.controller.handleCommand(audio(0, frame(3), { turnId: 'turn-active', attemptId: 'attempt-active' })), true);
  assert.deepEqual(fixture.connection.packets, [Buffer.from([1])]);
});

test('restores the durable exact-attempt revocation while allowing an unrelated later identity', () => {
  const attempted: string[] = [];
  const fixture = createFixture({
    onCancellation: () => true,
    isAttemptRevoked: (identity) => identity.turnId === turnId && identity.attemptId === attemptId,
    onPostCancellationPacket: (identity) => { attempted.push(`${identity.turnId}/${identity.attemptId}`); return true; }
  });
  assert.equal(fixture.controller.handleCommand(start()), true);
  assert.equal(fixture.controller.handleCommand(audio(0, frame(1))), true);
  assert.deepEqual(attempted, [`${turnId}/${attemptId}`]);
  assert.deepEqual(fixture.connection.packets, []);

  assert.equal(fixture.controller.handleCommand(start({ turnId: 'turn-later', attemptId: 'attempt-later' })), true);
  assert.equal(fixture.controller.handleCommand(audio(0, frame(2), { turnId: 'turn-later', attemptId: 'attempt-later' })), true);
  assert.deepEqual(fixture.connection.packets, [Buffer.from([1])]);
});

test('counts a late cancelled chunk before ignoring it while another attempt is active', () => {
  const attempted: string[] = [];
  const fixture = createFixture({
    onCancellation: () => true,
    isAttemptRevoked: (identity) => identity.turnId === turnId && identity.attemptId === attemptId,
    onPostCancellationPacket: (identity) => { attempted.push(`${identity.turnId}/${identity.attemptId}`); return true; }
  });
  fixture.controller.handleCommand(start({ turnId: 'turn-active', attemptId: 'attempt-active' }));
  assert.equal(fixture.controller.handleCommand(audio(0, frame(7))), true);
  assert.deepEqual(attempted, [`${turnId}/${attemptId}`]);
});

test('propagates durable attempted-counter failure and does not handle the rejected chunk', () => {
  const fixture = createFixture({
    onCancellation: () => true,
    isAttemptRevoked: () => true,
    onPostCancellationPacket: () => { throw new Error('fsync failed'); }
  });
  assert.throws(() => fixture.controller.handleCommand(audio(0, frame(1))), /fsync failed/);
  assert.deepEqual(fixture.connection.packets, []);
});

test('fails closed when the durable attempted-counter callback declines persistence', () => {
  const fixture = createFixture({
    onCancellation: () => true,
    isAttemptRevoked: () => true,
    onPostCancellationPacket: () => false
  });
  assert.equal(fixture.controller.handleCommand(audio(0, frame(1))), false);
  assert.deepEqual(fixture.connection.packets, []);
});

test('fails closed for malformed or non-integer playback cancellation v2 fields', () => {
  for (const overrides of [
    { meetingId: '' }, { reason: '' }, { cancellationObservedAtMs: -1 },
    { cancellationObservedAtMs: 1.5 },
    { cancellationObservedAtMs: CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS + 1 },
    { cancellationObservedAtMs: Number.MAX_SAFE_INTEGER }, { cancellationObservedAtMs: Number.MAX_SAFE_INTEGER + 1 },
    { unexpected: true }
  ]) {
    const fixture = createFixture();
    fixture.controller.handleCommand(start());
    assert.equal(fixture.controller.handleCommand({
      schemaVersion: 2, type: 'playback-cancel', meetingId: 'meeting-1', recordingId, turnId, attemptId,
      cancellationObservedAtMs: 1_776_124_803_000, reason: 'barge-in', ...overrides
    }), false);
  }
});

test('accepts and preserves the maximum four-digit canonical cancellation timestamp', () => {
  let observed: number | undefined;
  const fixture = createFixture({ onCancellation: (command) => {
    observed = command.cancellationObservedAtMs;
    return true;
  } });
  fixture.controller.handleCommand(start());
  assert.equal(fixture.controller.handleCommand({
    schemaVersion: 2, type: 'playback-cancel', meetingId: 'meeting-1', recordingId, turnId, attemptId,
    cancellationObservedAtMs: CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS, reason: 'barge-in'
  }), true);
  assert.equal(observed, CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS);
  assert.equal(new Date(observed!).toISOString(), '9999-12-31T23:59:59.999Z');
});

test('rejects an expanded-year cancellation timestamp before durable admission', () => {
  let durabilityCalls = 0;
  const fixture = createFixture({
    onCancellation: () => {
      durabilityCalls++;
      return true;
    }
  });
  fixture.controller.handleCommand(start());
  assert.equal(fixture.controller.handleCommand({
    schemaVersion: 2, type: 'playback-cancel', meetingId: 'meeting-1', recordingId, turnId, attemptId,
    cancellationObservedAtMs: CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS + 1, reason: 'barge-in'
  }), false);
  assert.equal(durabilityCalls, 0);
});

test('does not record a packet when direct Discord playback rejects it', () => {
  const { connection, controller, dispatchedPackets, events } = createFixture();
  connection.sendError = new Error('UDP send unavailable');

  controller.handleCommand(start());
  controller.handleCommand(audio(0, frame(1)));

  assert.deepEqual(connection.packets, []);
  assert.deepEqual(dispatchedPackets, []);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-failed']
  );
  assert.equal((events[0] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).code, 'playback-error');
});

test('keeps direct playback running if authoritative recording throws', () => {
  const { connection, controller, events } = createFixture(() => {
    throw new Error('recording disk failure');
  });

  controller.handleCommand(start());
  controller.handleCommand(audio(0, frame(1)));

  assert.deepEqual(connection.packets, [Buffer.from([1])]);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-started']
  );
});

test('finishes exactly once after the final direct send is accepted', () => {
  const { connection, controller, events } = createFixture();

  controller.handleCommand(start());
  controller.handleCommand(audio(0, frame(1)));
  controller.handleCommand(finish());
  controller.handleCommand(finish());

  assert.equal(connection.stopCalls, 0);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-started', 'playback-finished']
  );
  assert.deepEqual(connection.speaking, [true, false]);
});

test('connection loss fails the current turn and ignores later traffic', () => {
  const { connection, controller, dispatchedPackets, events, timer } = createFixture();

  controller.handleCommand(start());
  controller.handleCommand(audio(0, Buffer.concat([frame(1), frame(2)])));
  controller.connectionUnavailable();
  controller.handleCommand(audio(1, frame(3)));
  controller.handleCommand(finish());
  timer.advance(1_000);

  assert.equal(connection.stopCalls, 0);
  assert.deepEqual(connection.packets, [Buffer.from([1])]);
  assert.deepEqual(dispatchedPackets, [Buffer.from([1])]);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['playback-started', 'playback-failed']
  );
  assert.equal((events[1] as Extract<CraigPlaybackEvent, { type: 'playback-failed' }>).code, 'connection-unavailable');
});

test('shares the voice player with the now-recording announcement without routing Botik through Piper', () => {
  const { arbiter, connection, controller } = createFixture();

  assert.equal(arbiter.playNowRecording('/tmp/now-recording.opus'), true);
  controller.handleCommand(start());
  controller.handleCommand(audio(0, frame(1)));

  assert.deepEqual(
    connection.plays.map(({ options }) => options.format),
    ['ogg']
  );
  assert.equal(connection.stopCalls, 1);
  assert.equal(arbiter.playNowRecording('/tmp/second-announcement.opus'), false);

  controller.handleCommand(cancel());
  assert.equal(connection.stopCalls, 1);
  assert.equal(arbiter.playNowRecording('/tmp/after-cancel.opus'), true);
});

test('creates Botik metadata and preserves direct-track Ogg sequence and timing', () => {
  const botSnowflake = '1533228054724346087';
  const track = createAuthoritativeBotikPlaybackTrack(botSnowflake, 17);
  const first = appendAuthoritativeBotikPlaybackPacket(track, Buffer.from([1]), 48_000);
  const second = appendAuthoritativeBotikPlaybackPacket(track, Buffer.from([2]), 48_001);
  const afterGap = appendAuthoritativeBotikPlaybackPacket(track, Buffer.from([3]), 96_000);

  assert.deepEqual(track.user, {
    id: botSnowflake,
    username: 'Botik',
    discriminator: '0',
    globalName: 'Botik',
    bot: true,
    unknown: false,
    track: 17,
    packet: 8
  });
  assert.deepEqual(
    [first, second, afterGap].map(({ packetNo, chunk }) => ({ packetNo, time: chunk.time, timestamp: chunk.timestamp, data: chunk.data })),
    [
      { packetNo: 2, time: 48_000, timestamp: 48_000, data: Buffer.from([1]) },
      { packetNo: 4, time: 48_960, timestamp: 48_960, data: Buffer.from([2]) },
      { packetNo: 6, time: 96_000, timestamp: 49_920, data: Buffer.from([3]) }
    ]
  );
});

test('direct frame conversion rejects a non-20ms source frame', () => {
  assert.throws(() => duplicateMonoPcmFrameToStereo(Buffer.alloc(CRAIG_PLAYBACK_MONO_FRAME_BYTES - 2)), /1920-byte/);
});

test('the production native encoder produces a decodable Discord Opus packet', () => {
  const connection = new FakeVoiceConnection();
  const events: CraigPlaybackEvent[] = [];
  const controller = new CraigPlaybackController({
    recordingId,
    arbiter: new CraigPlaybackArbiter(() => connection),
    now: () => 4_000,
    onCancellation: () => true,
    isAttemptRevoked: () => false,
    onPostCancellationPacket: () => true,
    reliabilityStore: new MemoryPlaybackReliabilityStore(),
    onEvent: (event) => events.push(event)
  });
  const monoFrame = Buffer.alloc(CRAIG_PLAYBACK_MONO_FRAME_BYTES);
  for (let offset = 0; offset < monoFrame.length; offset += 2) monoFrame.writeInt16LE(2_000, offset);

  assert.equal(controller.handleCommand(start()), true);
  assert.equal(controller.handleCommand(audio(0, monoFrame)), true);
  const packet = connection.packets[0];
  assert.ok(packet);
  const decoded = new OpusEncoder(48_000, 2).decode(packet);

  assert.equal(decoded.byteLength, 960 * 2 * 2);
  assert.equal(events[0]?.type, 'playback-started');
});
