import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import {
  CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS,
  CraigPlaybackArbiter,
  CraigPlaybackOpusEncoder,
  CraigPlaybackVoiceConnection
} from './conversationPlayback';
import { ConversationPlaybackSocket, ConversationPlaybackSocketOptions, createConversationPlaybackSession } from './conversationPlaybackSession';

const recordingId = 'recording-1';
const guildId = '1533228590643155034';
const channelId = '1533228823045214398';

class FakeSocket extends EventEmitter {
  readonly sent: string[] = [];
  readonly closes: Array<{ code?: number; reason?: string | Buffer }> = [];

  send(data: string): void {
    this.sent.push(data);
  }

  close(code?: number, reason?: string | Buffer): void {
    this.closes.push({ code, reason });
    this.emit('close', code ?? 1000, typeof reason === 'string' ? Buffer.from(reason) : reason ?? Buffer.alloc(0));
  }
}

class FakeVoiceConnection implements CraigPlaybackVoiceConnection {
  readonly packets: Buffer[] = [];
  readonly speaking: boolean[] = [];
  ready = true;
  udpSocket: unknown | null = {};
  stopCalls = 0;

  play(): void {
    // Conversation playback uses sendAudioFrame directly, not Piper.play.
  }

  stopPlaying(): void {
    this.stopCalls++;
  }

  sendAudioFrame(packet: Buffer): void {
    this.packets.push(Buffer.from(packet));
  }

  setSpeaking(value: boolean): void {
    this.speaking.push(value);
  }
}

class FakeEncoder implements CraigPlaybackOpusEncoder {
  encode(): Buffer {
    return Buffer.from([0x42]);
  }
}

const logger = { debug: () => {}, warn: () => {} };

test('requires durable restart lookup and post-fence recording at the session composition boundary', async () => {
  await assert.rejects(
    // @ts-expect-error all durable cancellation ports are mandatory even when playback is disabled
    createConversationPlaybackSession({
      config: undefined,
      recordingId,
      guildId,
      channelId,
      arbiter: new CraigPlaybackArbiter(() => new FakeVoiceConnection()),
      logger,
      onCancellation: () => true
    }),
    /restart lookup, and post-fence attempt handlers are required/
  );
});

function start(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    schemaVersion: 1,
    recordingId,
    turnId: 'turn-1',
    attemptId: 'attempt-1',
    type: 'playback-start',
    format: 'pcm_s16le',
    sampleRateHz: 48_000,
    channels: 1,
    ...overrides
  };
}

function audio() {
  return {
    schemaVersion: 1,
    recordingId,
    turnId: 'turn-1',
    attemptId: 'attempt-1',
    type: 'audio-chunk',
    sequence: 0,
    pcmBase64: Buffer.alloc(1_920).toString('base64')
  };
}

function cancel(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    schemaVersion: 2,
    type: 'playback-cancel',
    meetingId: 'meeting-1',
    recordingId,
    turnId: 'turn-1',
    attemptId: 'attempt-1',
    cancellationObservedAtMs: 1_776_124_803_000,
    reason: 'barge-in',
    ...overrides
  };
}

async function nextTick(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

test('opens an authenticated recording-scoped outbound session and emits playback evidence', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-playback-session-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const tokenFile = path.join(root, 'token');
  await writeFile(tokenFile, 'service-token\n', { mode: 0o600 });

  const socket = new FakeSocket();
  const connection = new FakeVoiceConnection();
  const arbiter = new CraigPlaybackArbiter(() => connection);
  const dispatchedPackets: Buffer[] = [];
  let socketUrl = '';
  let socketOptions: ConversationPlaybackSocketOptions | undefined;
  const session = await createConversationPlaybackSession({
    config: {
      enabled: true,
      endpoint: 'wss://meeting.internal/v1/craig/playback?source=craig',
      tokenFile,
      connectionTimeoutMs: 5_000
    },
    recordingId,
    guildId,
    channelId,
    arbiter,
    logger,
    createGatewaySessionId: () => 'gateway-session-1',
    createOpusEncoder: () => new FakeEncoder(),
    now: () => 4_000,
    onCancellation: () => true,
    isAttemptRevoked: () => false,
    onPostCancellationPacket: () => true,
    onPacketDispatched: (packet) => dispatchedPackets.push(Buffer.from(packet)),
    socketFactory: (url, options) => {
      socketUrl = url;
      socketOptions = options;
      return socket as unknown as ConversationPlaybackSocket;
    }
  });
  assert.ok(session);

  socket.emit('open');
  socket.emit('message', JSON.stringify(start()), false);
  socket.emit('message', JSON.stringify(audio()), false);
  await nextTick();

  const endpoint = new URL(socketUrl);
  assert.equal(endpoint.searchParams.get('source'), 'craig');
  assert.equal(endpoint.searchParams.get('recordingId'), recordingId);
  assert.deepEqual(socketOptions, {
    headers: { Authorization: 'Bearer service-token' },
    handshakeTimeout: 5_000,
    maxPayload: 38_400
  });
  assert.deepEqual(connection.packets, [Buffer.from([0x42])]);
  assert.deepEqual(dispatchedPackets, [Buffer.from([0x42])]);
  assert.deepEqual(
    socket.sent.map((message) => JSON.parse(message).type),
    ['session-ready', 'playback-started']
  );
  assert.deepEqual(JSON.parse(socket.sent[0]!), {
    schemaVersion: 1,
    type: 'session-ready',
    recordingId,
    guildId,
    channelId,
    gatewaySessionId: 'gateway-session-1'
  });

  session.close('connection-unavailable');
  assert.equal(connection.stopCalls, 0);
  assert.deepEqual(connection.speaking, [true, false]);
  assert.deepEqual(socket.closes, [{ code: 1000, reason: 'connection-unavailable' }]);
  assert.equal(JSON.parse(socket.sent.at(-1)!).type, 'playback-failed');
});

test('closes the outbound transport for a cross-recording command', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-playback-protocol-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const tokenFile = path.join(root, 'token');
  await writeFile(tokenFile, 'service-token\n', { mode: 0o600 });

  const socket = new FakeSocket();
  let closeReason: string | undefined;
  const session = await createConversationPlaybackSession({
    config: {
      enabled: true,
      endpoint: 'ws://meeting.internal/v1/craig/playback',
      tokenFile,
      connectionTimeoutMs: 5_000
    },
    recordingId,
    guildId,
    channelId,
    arbiter: new CraigPlaybackArbiter(() => new FakeVoiceConnection()),
    logger,
    onCancellation: () => true,
    isAttemptRevoked: () => false,
    onPostCancellationPacket: () => true,
    onClosed: (reason) => {
      closeReason = reason;
    },
    socketFactory: () => socket as unknown as ConversationPlaybackSocket
  });
  assert.ok(session);

  socket.emit('open');
  socket.emit('message', JSON.stringify(start({ recordingId: 'another-recording' })), false);

  assert.equal(session.isClosed, true);
  assert.equal(closeReason, 'protocol-violation');
  assert.deepEqual(socket.closes, [{ code: 1008, reason: 'invalid playback protocol' }]);
});

test('closes fail-closed without leaking a durable late-counter exception from the socket listener', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-playback-counter-failure-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const tokenFile = path.join(root, 'token');
  await writeFile(tokenFile, 'service-token\n', { mode: 0o600 });

  const socket = new FakeSocket();
  const warnings: Array<{ message: string; error?: unknown }> = [];
  let closeReason: string | undefined;
  const session = await createConversationPlaybackSession({
    config: {
      enabled: true,
      endpoint: 'ws://meeting.internal/v1/craig/playback',
      tokenFile,
      connectionTimeoutMs: 5_000
    },
    recordingId,
    guildId,
    channelId,
    arbiter: new CraigPlaybackArbiter(() => new FakeVoiceConnection()),
    logger: {
      debug: () => {},
      warn: (message, error) => warnings.push({ message, error })
    },
    onCancellation: () => true,
    isAttemptRevoked: () => true,
    onPostCancellationPacket: () => {
      throw new Error('fsync failed');
    },
    onClosed: (reason) => {
      closeReason = reason;
    },
    socketFactory: () => socket as unknown as ConversationPlaybackSocket
  });
  assert.ok(session);

  socket.emit('open');
  assert.doesNotThrow(() => socket.emit('message', JSON.stringify(audio()), false));

  assert.equal(session.isClosed, true);
  assert.equal(closeReason, 'transport-disconnected');
  assert.deepEqual(socket.closes, [{ code: 1011, reason: 'transport error' }]);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0]!.message, /transport failed/);
  assert.match(String(warnings[0]!.error), /fsync failed/);
});

test('enforces the four-digit cancellation timestamp boundary before durable admission', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-playback-timestamp-boundary-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const tokenFile = path.join(root, 'token');
  await writeFile(tokenFile, 'service-token\n', { mode: 0o600 });

  const socket = new FakeSocket();
  const durableTimestamps: number[] = [];
  const session = await createConversationPlaybackSession({
    config: {
      enabled: true,
      endpoint: 'ws://meeting.internal/v1/craig/playback',
      tokenFile,
      connectionTimeoutMs: 5_000
    },
    recordingId,
    guildId,
    channelId,
    arbiter: new CraigPlaybackArbiter(() => new FakeVoiceConnection()),
    logger,
    onCancellation: (command) => {
      durableTimestamps.push(command.cancellationObservedAtMs!);
      return true;
    },
    isAttemptRevoked: () => false,
    onPostCancellationPacket: () => true,
    socketFactory: () => socket as unknown as ConversationPlaybackSocket
  });
  assert.ok(session);

  socket.emit('open');
  socket.emit('message', JSON.stringify(start()), false);
  socket.emit('message', JSON.stringify(cancel({
    cancellationObservedAtMs: CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS
  })), false);
  assert.equal(session.isClosed, false);
  assert.deepEqual(durableTimestamps, [CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS]);

  socket.emit('message', JSON.stringify(start({ turnId: 'turn-2', attemptId: 'attempt-2' })), false);
  socket.emit('message', JSON.stringify(cancel({
    turnId: 'turn-2',
    attemptId: 'attempt-2',
    cancellationObservedAtMs: CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS + 1
  })), false);
  assert.equal(session.isClosed, true);
  assert.deepEqual(durableTimestamps, [CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS]);
  assert.deepEqual(socket.closes, [{ code: 1008, reason: 'invalid playback protocol' }]);
});
