import assert from 'node:assert/strict';
import { test } from 'node:test';
import Module from 'node:module';
import {
  BoundedMeetingIntegrationSink,
  MeetingIntegrationDeliveryError,
  type MeetingVoicePacket
} from './meetingIntegration';

const packet: MeetingVoicePacket = {
  schemaVersion: 1, recordingId: 'recording-1', guildId: 'guild', channelId: 'channel',
  speakerId: 'speaker', rtpTimestamp: 100, rtpSequence: 1, receivedAtMs: 1000, relativeTimeMs: 0
};

for (const outcome of ['permanent', 'success', 'retry-success', 'retry-permanent'] as const) {
  test(`deferred local HTTP ${outcome} owns only the submitted batch, preserving appended tail and FIFO`, async () => {
    let settle!: (error?: Error) => void;
    const calls: Array<{ path: string; body: any }> = [];
    const sink = new BoundedMeetingIntegrationSink({
      async post(path, body) {
        calls.push({ path, body: JSON.parse(JSON.stringify(body)) });
        if (path === '/v1/craig/voice-packets' && calls.length === 1) await new Promise<void>((resolve, reject) => {
          settle = (error) => error ? reject(error) : resolve();
        });
        if (calls.length === 2 && outcome === 'retry-permanent')
          throw new MeetingIntegrationDeliveryError('local HTTP 400', false, 400);
      }
    }, { debug() {}, error() {}, warn() {} }, 4, 4);
    sink.publishLifecycle({
      schemaVersion: 1, eventId: 'start', recordingId: packet.recordingId,
      guildId: packet.guildId, channelId: packet.channelId,
      occurredAt: '2026-08-02T00:00:00.000Z', type: 'meeting.started', participantIds: [packet.speakerId]
    });
    assert.equal(await sink.drain(1000), true);
    calls.length = 0;
    const append = (sequence: number) => sink.publishPacket({
      ...packet, rtpSequence: sequence, rtpTimestamp: 100 + sequence * 960, relativeTimeMs: sequence * 20
    }, Buffer.from([sequence]));
    assert.equal(append(1), true);
    assert.equal(calls.length, 1);
    assert.equal(append(2), true);
    assert.equal(append(3), true);
    assert.equal(append(4), true);
    assert.equal(append(5), false);
    sink.publishLifecycle({
      schemaVersion: 1, eventId: 'end', recordingId: packet.recordingId,
      guildId: packet.guildId, channelId: packet.channelId,
      occurredAt: '2026-08-02T00:01:00.000Z', type: 'meeting.ended', reason: null
    });
    settle(outcome === 'success' ? undefined : new MeetingIntegrationDeliveryError(
      'deferred local HTTP response', outcome.startsWith('retry'), outcome.startsWith('retry') ? 503 : 400
    ));
    assert.equal(await sink.drain(2000), true);
    const voice = calls.filter(({ path }) => path === '/v1/craig/voice-packets');
    assert.deepEqual(voice.map(({ body }) => body.packets.map((p: MeetingVoicePacket) => p.rtpSequence)),
      outcome.startsWith('retry') ? [[1], [1], [2, 3, 4]] : [[1], [2, 3, 4]]);
    if (outcome.startsWith('retry')) assert.deepEqual(voice[0].body, voice[1].body);
    assert.equal(calls[calls.length - 1]?.path, '/v1/craig/events');
    assert.equal((sink as unknown as { queuedPackets: number }).queuedPackets, 0);
    assert.deepEqual(voice[voice.length - 1]?.body.packets.map((p: { opusBase64: string }) => p.opusBase64),
      [2, 3, 4].map((value) => Buffer.from([value]).toString('base64')));
  });
}

test('HTTP transport serializes only owned entries across a deferred 400 response (offline fetch)', async () => {
  const batches: number[][] = [];
  let settle!: (response: { ok: boolean; status: number }) => void;
  const loader = Module as unknown as { _load: (request: string, parent: unknown, main: boolean) => unknown };
  const original = loader._load;
  const modulePath = require.resolve('./meetingIntegration');
  const cached = require.cache[modulePath];
  delete require.cache[modulePath];
  loader._load = (request, parent, main) => request === 'node-fetch'
    ? async (url: string, init: { body: string }) => {
      if (url.endsWith('/v1/craig/voice-packets')) {
        const body = JSON.parse(init.body);
        batches.push(body.packets.map((item: MeetingVoicePacket) => item.rtpSequence));
        if (batches.length === 1) return new Promise((resolve) => { settle = resolve; });
      }
      return { ok: true, status: 204 };
    }
    : original(request, parent, main);
  let sink: BoundedMeetingIntegrationSink;
  try {
    const local = require('./meetingIntegration') as typeof import('./meetingIntegration');
    sink = new local.BoundedMeetingIntegrationSink(new local.HttpMeetingIntegrationTransport(
      new URL('http://craig-local.invalid'), 'local-test-token', 2000
    ), { debug() {}, error() {}, warn() {} }, 4, 4);
  } finally {
    loader._load = original;
    require.cache[modulePath] = cached;
  }
  sink.publishLifecycle({
    schemaVersion: 1, eventId: 'start', recordingId: packet.recordingId,
    guildId: packet.guildId, channelId: packet.channelId,
    occurredAt: '2026-08-02T00:00:00.000Z', type: 'meeting.started', participantIds: [packet.speakerId]
  });
  assert.equal(await sink.drain(1000), true);
  assert.equal(sink.publishPacket(packet, Buffer.from([1])), true);
  assert.equal(sink.publishPacket({ ...packet, rtpSequence: 2 }, Buffer.from([2])), true);
  settle({ ok: false, status: 400 });
  assert.equal(await sink.drain(1000), true);
  assert.deepEqual(batches, [[1], [2]]);
});
