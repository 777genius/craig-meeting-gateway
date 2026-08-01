import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  BoundedMeetingIntegrationSink,
  MeetingIntegrationLogger,
  MeetingIntegrationTransport,
  MeetingLifecycleEvent,
  MeetingTerminalLifecycle,
  MeetingVoicePacket
} from './meetingIntegration';

const logger: MeetingIntegrationLogger = {
  debug: () => {},
  error: () => {},
  warn: () => {}
};

const event: MeetingLifecycleEvent = {
  schemaVersion: 1,
  eventId: 'recording-1:1',
  recordingId: 'recording-1',
  guildId: '1533228590643155034',
  channelId: '1533228823045214398',
  occurredAt: '2026-08-02T00:00:00.000Z',
  type: 'meeting.started',
  participantIds: ['1533227577286852649']
};

const packet: MeetingVoicePacket = {
  schemaVersion: 1,
  recordingId: 'recording-1',
  guildId: '1533228590643155034',
  channelId: '1533228823045214398',
  speakerId: '1533227577286852649',
  rtpTimestamp: 1234,
  rtpSequence: 12,
  receivedAtMs: 1000,
  relativeTimeMs: 20
};

test('preserves lifecycle and accepted voice ordering while batching packets', async () => {
  const calls: Array<{ path: string; body: any }> = [];
  const transport: MeetingIntegrationTransport = {
    async post(path, body) {
      calls.push({ path, body });
    }
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 8, 2);

  assert.equal(sink.publishLifecycle(event), true);
  assert.equal(sink.publishPacket(packet, Buffer.from([1, 2, 3])), true);
  assert.equal(sink.publishPacket({ ...packet, rtpSequence: 13 }, Buffer.from([4, 5])), true);
  assert.equal(sink.publishLifecycle({ ...event, eventId: 'recording-1:2', type: 'meeting.ended', reason: null }), true);
  assert.equal(await sink.drain(1000), true);

  assert.deepEqual(
    calls.map((call) => call.path),
    ['/v1/craig/events', '/v1/craig/voice-packets', '/v1/craig/events']
  );
  assert.equal(calls[1].body.packets.length, 2);
  assert.equal(calls[1].body.packets[0].opusBase64, 'AQID');
});

test('admits packets synchronously, clones after admission, and rejects overflow', async () => {
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const calls: any[] = [];
  const transport: MeetingIntegrationTransport = {
    async post(_path, body) {
      calls.push(body);
      await blocked;
    }
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 1, 1);
  const opus = Buffer.from([7, 8, 9]);

  assert.equal(sink.publishPacket(packet, opus), true);
  opus[0] = 99;
  assert.equal(sink.publishPacket({ ...packet, rtpSequence: 13 }, Buffer.from([1])), false);
  release!();
  assert.equal(await sink.drain(1000), true);
  assert.equal(calls[0].packets[0].opusBase64, 'BwgJ');
});

test('retries transient delivery failures without removing or reordering data', async () => {
  let attempts = 0;
  const delivered: string[] = [];
  const transport: MeetingIntegrationTransport = {
    async post(path) {
      attempts++;
      if (attempts === 1) throw new Error('temporary outage');
      delivered.push(path);
    }
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 4, 2);

  sink.publishLifecycle(event);
  sink.publishPacket(packet, Buffer.from([1]));

  assert.equal(await sink.drain(2000), true);
  assert.equal(attempts, 3);
  assert.deepEqual(delivered, ['/v1/craig/events', '/v1/craig/voice-packets']);
});

test('publishes one aborted terminal lifecycle when recording finalization fails', async () => {
  const terminal = new MeetingTerminalLifecycle();
  const published: string[] = [];

  await assert.rejects(
    terminal.complete(
      'meeting.ended',
      async () => {
        throw new Error('writer end failed');
      },
      (type) => published.push(type)
    ),
    /writer end failed/
  );
  terminal.abort((type) => published.push(type));

  assert.deepEqual(published, ['meeting.aborted']);
});

test('never replaces a published ended lifecycle with a later abort', async () => {
  const terminal = new MeetingTerminalLifecycle();
  const published: string[] = [];

  await terminal.complete(
    'meeting.ended',
    async () => undefined,
    (type) => published.push(type)
  );
  terminal.abort((type) => published.push(type));

  assert.deepEqual(published, ['meeting.ended']);
});
