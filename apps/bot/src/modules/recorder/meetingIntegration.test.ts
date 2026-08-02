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

test('rejects derived traffic outside an open meeting lifecycle', async () => {
  const calls: string[] = [];
  const transport: MeetingIntegrationTransport = {
    async post(path) {
      calls.push(path);
    }
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 8, 2);
  const participantJoined: MeetingLifecycleEvent = {
    ...event,
    eventId: 'recording-1:2',
    participantId: '1533228054724346087',
    participantIds: undefined,
    type: 'participant.joined'
  };

  assert.equal(sink.publishPacket(packet, Buffer.from([1])), false);
  assert.equal(sink.publishLifecycle(participantJoined), false);
  assert.equal(sink.publishLifecycle(event), true);
  assert.equal(sink.publishPacket(packet, Buffer.from([2])), true);
  assert.equal(sink.publishLifecycle({ ...event, eventId: 'recording-1:3', type: 'meeting.ended', reason: null }), true);
  assert.equal(sink.publishPacket({ ...packet, rtpSequence: 13 }, Buffer.from([3])), false);
  assert.equal(sink.publishLifecycle({ ...participantJoined, eventId: 'recording-1:4' }), false);
  assert.equal(await sink.drain(1000), true);
  assert.deepEqual(calls, ['/v1/craig/events', '/v1/craig/voice-packets', '/v1/craig/events']);
});

test('reserves terminal capacity when lifecycle traffic reaches its bound', async () => {
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const calls: MeetingLifecycleEvent[] = [];
  const transport: MeetingIntegrationTransport = {
    async post(path, body) {
      if (path === '/v1/craig/events') calls.push(body as MeetingLifecycleEvent);
      await blocked;
    }
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 8, 2, 3);
  const joined: MeetingLifecycleEvent = {
    ...event,
    eventId: 'recording-1:2',
    participantId: '1533228054724346087',
    participantIds: undefined,
    type: 'participant.joined'
  };

  assert.equal(sink.publishLifecycle(event), true);
  assert.equal(sink.publishLifecycle(joined), true);
  assert.equal(sink.publishLifecycle({ ...joined, eventId: 'recording-1:3' }), false);
  assert.equal(sink.publishLifecycle({ ...event, eventId: 'recording-1:4', type: 'meeting.ended' }), true);
  release!();
  assert.equal(await sink.drain(1000), true);
  assert.deepEqual(
    calls.map(({ type }) => type),
    ['meeting.started', 'participant.joined', 'meeting.ended']
  );
});

test('tracks interleaved recording lifecycles independently', async () => {
  const transport: MeetingIntegrationTransport = { post: async () => undefined };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 8, 2);
  const secondStart: MeetingLifecycleEvent = {
    ...event,
    eventId: 'recording-2:1',
    recordingId: 'recording-2'
  };

  assert.equal(sink.publishLifecycle(event), true);
  assert.equal(sink.publishLifecycle(secondStart), true);
  assert.equal(sink.publishLifecycle({ ...event, eventId: 'recording-1:2', type: 'meeting.ended' }), true);
  assert.equal(sink.publishPacket({ ...packet, recordingId: 'recording-2' }, Buffer.from([1])), true);
  assert.equal(sink.publishPacket(packet, Buffer.from([2])), false);
  assert.equal(sink.publishLifecycle({ ...secondStart, eventId: 'recording-2:2', type: 'meeting.ended' }), true);
  assert.equal(await sink.drain(1000), true);
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

  assert.equal(sink.publishLifecycle(event), true);
  assert.equal(sink.publishPacket(packet, opus), true);
  opus[0] = 99;
  assert.equal(sink.publishPacket({ ...packet, rtpSequence: 13 }, Buffer.from([1])), false);
  release!();
  assert.equal(await sink.drain(1000), true);
  assert.equal(calls[1].packets[0].opusBase64, 'BwgJ');
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
