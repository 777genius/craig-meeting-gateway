import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, rm, unlink, writeFile } from 'node:fs/promises';
import { type IncomingHttpHeaders, createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import {
  AuthoritativeRecordingReadyEvent,
  AuthoritativeTrackMetadata,
  BoundedMeetingIntegrationSink,
  CookedAuthoritativeTrack,
  HttpMeetingIntegrationTransport,
  isRetryableMeetingIntegrationStatus,
  MeetingIntegrationDeliveryError,
  MeetingIntegrationLogger,
  MeetingIntegrationTransport,
  MeetingLifecycleEvent,
  MeetingStartedLifecycleEvent,
  MeetingTerminalLifecycle,
  MeetingTerminalLifecycleEvent,
  MeetingVoicePacket,
  OriginalRecordingCooker
} from './meetingIntegration';

const logger: MeetingIntegrationLogger = {
  debug: () => {},
  error: () => {},
  warn: () => {}
};

const event: MeetingStartedLifecycleEvent = {
  schemaVersion: 1,
  eventId: 'recording-1:1',
  recordingId: 'recording-1',
  guildId: '1533228590643155034',
  channelId: '1533228823045214398',
  occurredAt: '2026-08-02T00:00:00.000Z',
  type: 'meeting.started',
  participantIds: ['1533227577286852649']
};

const terminalEvent: MeetingTerminalLifecycleEvent = {
  schemaVersion: 1,
  eventId: 'recording-1:2',
  recordingId: 'recording-1',
  guildId: '1533228590643155034',
  channelId: '1533228823045214398',
  occurredAt: '2026-08-02T00:01:00.000Z',
  type: 'meeting.ended',
  reason: null
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

test('permanent delivery rejection advances the realtime FIFO without retrying the rejected item', async () => {
  let attempts = 0;
  const delivered: string[] = [];
  const transport: MeetingIntegrationTransport = {
    async post(path) {
      attempts++;
      if (attempts === 1) throw new MeetingIntegrationDeliveryError('bad request', false, 400);
      delivered.push(path);
    }
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 4, 2);

  sink.publishLifecycle(event);
  sink.publishPacket(packet, Buffer.from([1]));
  sink.publishLifecycle({ ...event, eventId: 'recording-1:2', type: 'meeting.ended' });

  assert.equal(await sink.drain(1000), true);
  assert.equal(attempts, 3);
  assert.deepEqual(delivered, ['/v1/craig/voice-packets', '/v1/craig/events']);
});

test('retries only the explicitly recoverable HTTP statuses', () => {
  for (const status of [408, 409, 425, 429, 500, 503, 599]) assert.equal(isRetryableMeetingIntegrationStatus(status), true, String(status));
  for (const status of [300, 400, 401, 403, 404, 410, 422, 499]) assert.equal(isRetryableMeetingIntegrationStatus(status), false, String(status));
});

test('HTTP original recording contract streams audio metadata and requires ready 202', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-original-http-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const audioFilePath = path.join(root, 'track.ogg');
  const audio = Buffer.from('OggS-authoritative-track');
  await writeFile(audioFilePath, audio);
  let readyStatus = 200;
  const requests: Array<{ path: string; headers: IncomingHttpHeaders; body: Buffer }> = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    requests.push({ path: request.url ?? '', headers: request.headers, body: Buffer.concat(chunks) });
    response.writeHead(request.url === '/v1/craig/events' ? readyStatus : 202).end();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  context.after(async () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))));
  const address = server.address() as AddressInfo;
  const transport = new HttpMeetingIntegrationTransport(new URL(`http://127.0.0.1:${address.port}`), 'test-token', 1000);
  const metadata: AuthoritativeTrackMetadata = {
    schemaVersion: 1,
    uploadId: 'authoritative-track:v1:recording-1:1',
    recordingId: 'recording-1',
    guildId: event.guildId,
    channelId: event.channelId,
    speakerId: packet.speakerId,
    trackNumber: 1,
    timelineOffsetMs: 0,
    checksumSha256: createHash('sha256').update(audio).digest('hex'),
    sizeBytes: audio.length
  };
  await transport.postAuthoritativeTrack(metadata, audioFilePath);

  const encodedMetadata = requests[0]?.headers['x-craig-authoritative-track-metadata'];
  assert.equal(requests[0]?.path, '/v1/craig/authoritative-tracks');
  assert.equal(requests[0]?.headers['content-type'], 'audio/ogg');
  assert.deepEqual(requests[0]?.body, audio);
  assert.deepEqual(JSON.parse(Buffer.from(String(encodedMetadata), 'base64url').toString('utf8')), metadata);

  const ready: AuthoritativeRecordingReadyEvent = {
    schemaVersion: 1,
    eventId: 'recording-1:authoritative-ready:v1',
    recordingId: 'recording-1',
    guildId: event.guildId,
    channelId: event.channelId,
    occurredAt: '2026-08-02T00:01:00.000Z',
    type: 'recording.authoritative_ready',
    endedAt: '2026-08-02T00:01:00.000Z',
    trackCount: 1,
    sourceFilesChecksumSha256: 'a'.repeat(64)
  };
  await assert.rejects(transport.postAuthoritativeReady(ready), (error: unknown) => {
    assert.ok(error instanceof MeetingIntegrationDeliveryError);
    assert.equal(error.status, 200);
    assert.equal(error.retryable, false);
    return true;
  });
  readyStatus = 202;
  await transport.postAuthoritativeReady(ready);
});

test('restores exact lifecycle events before tracks after losing the realtime queue', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-original-outbox-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  const sourceFileBase = path.join(recordingRoot, 'recording-1.ogg');
  await mkdir(recordingRoot, { recursive: true });
  const sources: Record<string, string | Buffer> = {
    data: Buffer.from('original-data'),
    header1: Buffer.from('original-header-1'),
    header2: Buffer.from('original-header-2'),
    users: '"0":{}\n,"1":{"id":"1533227577286852649"}\n,"2":{"id":"1533228054724346087"}\n',
    info: '{"format":1}',
    log: 'closed\n'
  };
  await Promise.all(Object.entries(sources).map(([kind, contents]) => writeFile(`${sourceFileBase}.${kind}`, contents)));

  const first = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, {
    recordingRoot,
    outboxRoot
  });
  assert.equal(
    await first.publishOriginalRecording({
      startedEvent: event,
      terminalEvent,
      sourceFileBase
    }),
    true
  );
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending')), ['recording-1.json']);

  const uploads: AuthoritativeTrackMetadata[] = [];
  const readyEvents: AuthoritativeRecordingReadyEvent[] = [];
  const lifecycleEvents: MeetingLifecycleEvent[] = [];
  const deliveryOrder: string[] = [];
  let readyAttempts = 0;
  const cooker: OriginalRecordingCooker = {
    async cook(recordingId, trackNumber): Promise<CookedAuthoritativeTrack> {
      const filePath = path.join(root, `${recordingId}-${trackNumber}-${uploads.length}.ogg`);
      const bytes = Buffer.from(`OggS-track-${trackNumber}`);
      await writeFile(filePath, bytes);
      return {
        filePath,
        checksumSha256: createHash('sha256').update(bytes).digest('hex'),
        sizeBytes: bytes.length,
        dispose: async () => unlink(filePath).catch(() => undefined)
      };
    }
  };
  const transport: MeetingIntegrationTransport = {
    async post(path, body) {
      assert.equal(path, '/v1/craig/events');
      const lifecycle = body as MeetingLifecycleEvent;
      lifecycleEvents.push(lifecycle);
      deliveryOrder.push(`event:${lifecycle.type}`);
    },
    async postAuthoritativeTrack(metadata, audioFilePath) {
      assert.equal((await readFile(audioFilePath)).subarray(0, 4).toString('ascii'), 'OggS');
      uploads.push(metadata);
      deliveryOrder.push(`track:${metadata.trackNumber}`);
    },
    async postAuthoritativeReady(ready) {
      readyAttempts++;
      deliveryOrder.push('ready');
      if (readyAttempts === 1) throw new Error('network reset after uploads');
      readyEvents.push(ready);
    }
  };
  const restored = new BoundedMeetingIntegrationSink(transport, logger, 4, 2, 1024, {
    recordingRoot,
    outboxRoot,
    cooker
  });
  await restored.restoreOriginalRecordingJobs();

  assert.equal(await restored.drain(2000), true);
  assert.equal(readyAttempts, 2);
  assert.deepEqual(lifecycleEvents, [event, terminalEvent, event, terminalEvent]);
  assert.deepEqual(deliveryOrder, [
    'event:meeting.started',
    'event:meeting.ended',
    'track:1',
    'track:2',
    'ready',
    'event:meeting.started',
    'event:meeting.ended',
    'track:1',
    'track:2',
    'ready'
  ]);
  assert.deepEqual(
    uploads.map(({ speakerId, trackNumber, timelineOffsetMs }) => ({ speakerId, trackNumber, timelineOffsetMs })),
    [
      { speakerId: '1533227577286852649', trackNumber: 1, timelineOffsetMs: 0 },
      { speakerId: '1533228054724346087', trackNumber: 2, timelineOffsetMs: 0 },
      { speakerId: '1533227577286852649', trackNumber: 1, timelineOffsetMs: 0 },
      { speakerId: '1533228054724346087', trackNumber: 2, timelineOffsetMs: 0 }
    ]
  );
  assert.equal(new Set(uploads.map(({ uploadId }) => uploadId)).size, 2);
  assert.equal(readyEvents[0]?.trackCount, 2);
  assert.match(readyEvents[0]?.sourceFilesChecksumSha256 ?? '', /^[0-9a-f]{64}$/);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending')), []);
});

test('rejects legacy outbox jobs without deleting original Craig files', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-legacy-outbox-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  const pendingRoot = path.join(outboxRoot, 'pending');
  const originalPath = path.join(recordingRoot, 'recording-legacy.ogg.data');
  await mkdir(pendingRoot, { recursive: true });
  await mkdir(recordingRoot, { recursive: true });
  await writeFile(originalPath, 'authoritative-original');
  await writeFile(
    path.join(pendingRoot, 'recording-legacy.json'),
    JSON.stringify({
      schemaVersion: 1,
      publicationId: 'authoritative-recording:v1:recording-legacy',
      recordingId: 'recording-legacy'
    })
  );

  const sink = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, {
    recordingRoot,
    outboxRoot
  });
  await sink.restoreOriginalRecordingJobs();

  assert.deepEqual(await readdir(pendingRoot), []);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'rejected')), ['recording-legacy.json']);
  assert.equal(await readFile(originalPath, 'utf8'), 'authoritative-original');
});

test('rejects a corrupt first original job without blocking the next recording', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-poison-outbox-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  await mkdir(recordingRoot, { recursive: true });

  const writeSources = async (recordingId: string, users: string): Promise<string> => {
    const sourceFileBase = path.join(recordingRoot, `${recordingId}.ogg`);
    await Promise.all(
      Object.entries({
        data: Buffer.from(`original-data-${recordingId}`),
        header1: Buffer.from('original-header-1'),
        header2: Buffer.from('original-header-2'),
        users,
        info: '{"format":1}',
        log: 'closed\n'
      }).map(([kind, contents]) => writeFile(`${sourceFileBase}.${kind}`, contents))
    );
    return sourceFileBase;
  };
  const corruptBase = await writeSources('recording-1', '"0":{}\n,"1":{"id":\n');
  const validBase = await writeSources('recording-2', '"0":{}\n,"1":{"id":"1533227577286852649"}\n');
  const secondStarted: MeetingStartedLifecycleEvent = {
    ...event,
    eventId: 'recording-2:1',
    recordingId: 'recording-2',
    occurredAt: '2026-08-02T00:01:30.000Z'
  };
  const secondTerminal: MeetingTerminalLifecycleEvent = {
    ...terminalEvent,
    eventId: 'recording-2:2',
    recordingId: 'recording-2',
    occurredAt: '2026-08-02T00:02:00.000Z'
  };
  const staging = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, {
    recordingRoot,
    outboxRoot
  });
  assert.equal(await staging.publishOriginalRecording({ startedEvent: event, terminalEvent, sourceFileBase: corruptBase }), true);
  assert.equal(
    await staging.publishOriginalRecording({ startedEvent: secondStarted, terminalEvent: secondTerminal, sourceFileBase: validBase }),
    true
  );

  const uploadedRecordings: string[] = [];
  const readyRecordings: string[] = [];
  const cooker: OriginalRecordingCooker = {
    async cook(recordingId, trackNumber): Promise<CookedAuthoritativeTrack> {
      const filePath = path.join(root, `${recordingId}-${trackNumber}.ogg`);
      const bytes = Buffer.from(`OggS-${recordingId}-${trackNumber}`);
      await writeFile(filePath, bytes);
      return {
        filePath,
        checksumSha256: createHash('sha256').update(bytes).digest('hex'),
        sizeBytes: bytes.length,
        dispose: async () => unlink(filePath).catch(() => undefined)
      };
    }
  };
  const restored = new BoundedMeetingIntegrationSink(
    {
      post: async () => undefined,
      async postAuthoritativeTrack(metadata) {
        uploadedRecordings.push(metadata.recordingId);
      },
      async postAuthoritativeReady(ready) {
        readyRecordings.push(ready.recordingId);
      }
    },
    logger,
    4,
    2,
    1024,
    { recordingRoot, outboxRoot, cooker }
  );
  await restored.restoreOriginalRecordingJobs();

  assert.equal(await restored.drain(2000), true);
  assert.deepEqual(uploadedRecordings, ['recording-2']);
  assert.deepEqual(readyRecordings, ['recording-2']);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending')), []);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'rejected')), ['recording-1.json']);
  assert.equal(await readFile(`${corruptBase}.data`, 'utf8'), 'original-data-recording-1');
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
