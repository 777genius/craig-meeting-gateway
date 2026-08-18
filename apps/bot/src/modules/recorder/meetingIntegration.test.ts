import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, rm, unlink, writeFile } from 'node:fs/promises';
import { type IncomingHttpHeaders, createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import crc32 from './crc32';
import {
  type AuthoritativeRecordingReadyEvent,
  type AuthoritativeTrackMetadata,
  type CookedAuthoritativeTrack,
  type MeetingIntegrationLogger,
  type MeetingIntegrationTransport,
  type MeetingLifecycleEvent,
  type MeetingLifecyclePublishOutcome,
  type MeetingStartedLifecycleEvent,
  type MeetingTerminalLifecycleEvent,
  type MeetingVoicePacket,
  type OriginalRecordingCooker,
  BoundedMeetingIntegrationSink,
  dateMillisecondsToIsoOrThrow,
  HttpMeetingIntegrationTransport,
  isRetryableMeetingIntegrationStatus,
  MeetingIntegrationDeliveryError,
  MeetingTerminalLifecycle,
  parseMeetingPlatformConfiguration,
  reportMeetingLifecyclePublishOutcome
} from './meetingIntegration';
import { actorSemanticsVersion, createCraigLifecycleV3Producer, sealedActorRosterCapabilityId } from './meetingLifecycleV3';

const logger: MeetingIntegrationLogger = {
  debug: () => {},
  error: () => {},
  warn: () => {}
};

const accepted: MeetingLifecyclePublishOutcome = { status: 'accepted' };
const uploadAck = (metadata: AuthoritativeTrackMetadata) => ({
  schemaVersion: 1 as const, uploadId: metadata.uploadId, recordingId: metadata.recordingId,
  trackNumber: metadata.trackNumber, checksumSha256: metadata.checksumSha256, sizeBytes: metadata.sizeBytes,
  durable: true as const, immutable: true as const,
  object: { provider: 's3' as const, bucket: 'botik-final', key: `${metadata.recordingId}/${metadata.trackNumber}.ogg`, versionId: 'version-1' }
});
const proofReceipt = (proof: unknown, acknowledgement: ReturnType<typeof uploadAck>) => ({
  schemaVersion: 1 as const,
  proofId: createHash('sha256').update(JSON.stringify(proof)).digest('hex'),
  object: acknowledgement.object,
  sizeBytes: acknowledgement.sizeBytes,
  checksumSha256: acknowledgement.checksumSha256
});
const lifecycleV3Config = {
  schemaVersion: 3 as const,
  actorSemanticsVersion,
  producerCapabilityId: sealedActorRosterCapabilityId,
  producerRevision: '0123456789abcdef0123456789abcdef01234567'
};

test('cancellation timestamp conversion accepts the four-digit year maximum and rejects expanded years', () => {
  assert.equal(dateMillisecondsToIsoOrThrow(253_402_300_799_999, 'cancellationObservedAtMs'), '9999-12-31T23:59:59.999Z');
  assert.throws(
    () => dateMillisecondsToIsoOrThrow(253_402_300_800_000, 'cancellationObservedAtMs'),
    /outside the canonical four-digit year range/
  );
  assert.throws(
    () => dateMillisecondsToIsoOrThrow(Number.MAX_SAFE_INTEGER, 'cancellationObservedAtMs'),
    /outside the JavaScript Date range/
  );
});

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

function rawOggPage(granule: number, trackNumber: number, sequenceNumber: number, packet: Buffer, headerType = 0): Buffer {
  const segments: number[] = [];
  let remaining = packet.byteLength;
  while (remaining >= 255) {
    segments.push(255);
    remaining -= 255;
  }
  segments.push(remaining);
  const page = Buffer.alloc(27 + segments.length + packet.byteLength);
  page.write('OggS');
  page.writeUInt8(headerType, 5);
  page.writeBigUInt64LE(BigInt(granule), 6);
  page.writeUInt32LE(trackNumber, 14);
  page.writeUInt32LE(sequenceNumber, 18);
  page.writeUInt8(segments.length, 26);
  Buffer.from(segments).copy(page, 27);
  packet.copy(page, 27 + segments.length);
  page.writeInt32LE(crc32(page), 22);
  return page;
}

async function runNative(command: string, args: string[], input?: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
    child.once('error', reject);
    child.once('close', (code, signal) => {
      if (code === 0) resolve(Buffer.concat(stdout));
      else reject(new Error(`${command} failed (${code ?? signal ?? 'unknown'}): ${Buffer.concat(stderr).toString('utf8')}`));
    });
    child.stdin.end(input);
  });
}

function oggCrc32(bytes: Buffer): number {
  let crc = 0;
  for (const value of bytes) {
    crc ^= value << 24;
    for (let bit = 0; bit < 8; bit++) {
      crc = ((crc << 1) ^ ((crc & 0x8000_0000) === 0 ? 0 : 0x04c1_1db7)) >>> 0;
    }
  }
  return crc;
}

function inspectOggPages(bytes: Buffer): Array<{ body: Buffer; granule: bigint; sequence: number; type: number }> {
  const pages: Array<{ body: Buffer; granule: bigint; sequence: number; type: number }> = [];
  let offset = 0;
  while (offset < bytes.length) {
    assert.equal(bytes.toString('ascii', offset, offset + 4), 'OggS');
    assert.ok(offset + 27 <= bytes.length);
    const segmentCount = bytes.readUInt8(offset + 26);
    assert.ok(offset + 27 + segmentCount <= bytes.length);
    let bodyLength = 0;
    for (let index = 0; index < segmentCount; index++) bodyLength += bytes.readUInt8(offset + 27 + index);
    const bodyOffset = offset + 27 + segmentCount;
    const end = bodyOffset + bodyLength;
    assert.ok(end <= bytes.length);
    const page = Buffer.from(bytes.subarray(offset, end));
    const checksum = page.readUInt32LE(22);
    page.fill(0, 22, 26);
    assert.equal(oggCrc32(page), checksum);
    pages.push({
      body: bytes.subarray(bodyOffset, end),
      granule: bytes.readBigUInt64LE(offset + 6),
      sequence: bytes.readUInt32LE(offset + 18),
      type: bytes.readUInt8(offset + 5)
    });
    offset = end;
  }
  return pages;
}

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

test('cooks a page-aligned Craig stream with end granules and one final EOS page', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-oggcorrect-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const executable = path.join(root, 'oggcorrect');
  const source = path.resolve(__dirname, '../../../../../cook/oggcorrect.c');
  await runNative(process.env.CC ?? 'cc', ['-O2', '-o', executable, source]);

  const opusHead = Buffer.alloc(19);
  opusHead.write('OpusHead');
  opusHead.writeUInt8(1, 8);
  opusHead.writeUInt8(1, 9);
  opusHead.writeUInt32LE(48_000, 12);
  const opusTags = Buffer.alloc(16);
  opusTags.write('OpusTags');
  const opusPacket = Buffer.from([0xf8, 0xff, 0xfe]);
  const sourcePages = [
    rawOggPage(0, 1, 0, opusHead, 0x02),
    rawOggPage(0, 1, 1, opusTags),
    rawOggPage(48_000, 1, 2, opusPacket),
    rawOggPage(48_960, 1, 3, opusPacket)
  ];
  const output = await runNative(executable, ['1'], Buffer.concat([...sourcePages, ...sourcePages]));
  const pages = inspectOggPages(output);

  assert.equal(pages.length, 4);
  assert.equal(pages[0]?.type, 0x02);
  assert.equal(pages[2]?.granule, 960n);
  assert.equal(pages[2]?.type, 0);
  assert.equal(pages[3]?.granule, 1920n);
  assert.equal(pages[3]?.type, 0x04);
  assert.equal(pages.filter(({ type }) => (type & 0x04) !== 0).length, 1);
  assert.deepEqual(pages[2]?.body, opusPacket);
  assert.deepEqual(pages[3]?.body, opusPacket);
});

test('keeps Opus headers and emits positive audio beyond pre-skip for a silent track', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-oggcorrect-silent-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const executable = path.join(root, 'oggcorrect');
  const source = path.resolve(__dirname, '../../../../../cook/oggcorrect.c');
  await runNative(process.env.CC ?? 'cc', ['-O2', '-o', executable, source]);

  const preSkip = 3_840;
  const opusHead = Buffer.alloc(19);
  opusHead.write('OpusHead');
  opusHead.writeUInt8(1, 8);
  opusHead.writeUInt8(1, 9);
  opusHead.writeUInt16LE(preSkip, 10);
  opusHead.writeUInt32LE(48_000, 12);
  const opusTags = Buffer.alloc(16);
  opusTags.write('OpusTags');
  const zeroPacket = Buffer.from([0xf8, 0xff, 0xfe]);
  const keepHeaders = [rawOggPage(0, 1, 0, opusHead, 0x02), rawOggPage(0, 1, 1, opusTags)];
  const otherStream = [rawOggPage(0, 2, 0, opusHead, 0x02), rawOggPage(0, 2, 1, opusTags), rawOggPage(48_000, 2, 2, zeroPacket)];
  const cycles = [Buffer.concat([...keepHeaders, ...otherStream]), Buffer.concat(keepHeaders)];

  for (const cycle of cycles) {
    const output = await runNative(executable, ['1'], Buffer.concat([cycle, cycle]));
    const pages = inspectOggPages(output);

    assert.equal(pages.length, 7);
    assert.deepEqual(pages[0]?.body, opusHead);
    assert.deepEqual(pages[1]?.body, opusTags);
    assert.equal(pages[0]?.type, 0x02);
    assert.deepEqual(
      pages.map(({ sequence }) => sequence),
      [0, 1, 2, 3, 4, 5, 6]
    );
    assert.deepEqual(
      pages.slice(2).map(({ granule }) => granule),
      [960n, 1_920n, 2_880n, 3_840n, 4_800n]
    );
    assert.ok((pages.at(-1)?.granule ?? 0n) > BigInt(preSkip));
    assert.deepEqual(
      pages.slice(2).map(({ body }) => body),
      [zeroPacket, zeroPacket, zeroPacket, zeroPacket, zeroPacket]
    );
    assert.equal(pages.filter(({ type }) => (type & 0x02) !== 0).length, 1);
    assert.equal(pages.filter(({ type }) => (type & 0x04) !== 0).length, 1);
    assert.equal(pages.at(-1)?.type, 0x04);
  }
});

test('preserves lifecycle and accepted voice ordering while batching packets', async () => {
  const calls: Array<{ path: string; body: any }> = [];
  const transport: MeetingIntegrationTransport = {
    async post(path, body) {
      calls.push({ path, body });
    }
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 8, 2);

  assert.deepEqual(sink.publishLifecycle(event), accepted);
  assert.equal(sink.publishPacket(packet, Buffer.from([1, 2, 3])), true);
  assert.equal(sink.publishPacket({ ...packet, rtpSequence: 13 }, Buffer.from([4, 5])), true);
  assert.deepEqual(
    sink.publishLifecycle({
      ...event,
      eventId: 'recording-1:2',
      type: 'meeting.ended',
      reason: null
    }),
    accepted
  );
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
  assert.deepEqual(sink.publishLifecycle(participantJoined), { status: 'missing-start' });
  assert.deepEqual(sink.publishLifecycle(event), accepted);
  assert.equal(sink.publishPacket(packet, Buffer.from([2])), true);
  assert.deepEqual(
    sink.publishLifecycle({
      ...event,
      eventId: 'recording-1:3',
      type: 'meeting.ended',
      reason: null
    }),
    accepted
  );
  assert.equal(sink.publishPacket({ ...packet, rtpSequence: 13 }, Buffer.from([3])), false);
  assert.deepEqual(sink.publishLifecycle({ ...participantJoined, eventId: 'recording-1:4' }), { status: 'missing-start' });
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

  assert.deepEqual(sink.publishLifecycle(event), accepted);
  assert.deepEqual(sink.publishLifecycle(joined), accepted);
  assert.deepEqual(sink.publishLifecycle({ ...joined, eventId: 'recording-1:3' }), { status: 'capacity-exhausted' });
  assert.deepEqual(
    sink.publishLifecycle({
      ...event,
      eventId: 'recording-1:4',
      type: 'meeting.ended'
    }),
    accepted
  );
  release!();
  assert.equal(await sink.drain(1000), true);
  assert.deepEqual(
    calls.map(({ type }) => type),
    ['meeting.started', 'participant.joined', 'meeting.ended']
  );
});

test('classifies true lifecycle capacity exhaustion separately from duplicates and missing starts', async () => {
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const sink = new BoundedMeetingIntegrationSink(
    {
      async post() {
        await blocked;
      }
    },
    logger,
    8,
    2,
    2
  );
  const secondStart: MeetingStartedLifecycleEvent = {
    ...event,
    eventId: 'recording-2:1',
    recordingId: 'recording-2'
  };

  assert.deepEqual(sink.publishLifecycle(event), accepted);
  assert.deepEqual(sink.publishLifecycle(event), { status: 'duplicate' });
  assert.deepEqual(sink.publishLifecycle(secondStart), { status: 'capacity-exhausted' });
  assert.deepEqual(sink.publishLifecycle({ ...secondStart, eventId: 'recording-2:2', type: 'meeting.aborted', reason: 'not started' }), {
    status: 'missing-start'
  });
  assert.deepEqual(sink.publishLifecycle({ ...event, eventId: 'recording-1:2', type: 'meeting.aborted', reason: 'voice timeout' }), accepted);
  release!();
  assert.equal(await sink.drain(1000), true);
});

test('classifies a second terminal lifecycle as duplicate without delivering it', async () => {
  const delivered: MeetingLifecycleEvent[] = [];
  const sink = new BoundedMeetingIntegrationSink(
    {
      async post(path, body) {
        if (path === '/v1/craig/events') delivered.push(body as MeetingLifecycleEvent);
      }
    },
    logger,
    8,
    2
  );
  const aborted: MeetingTerminalLifecycleEvent = {
    ...terminalEvent,
    type: 'meeting.aborted',
    reason: 'voice timeout'
  };

  assert.deepEqual(sink.publishLifecycle(event), accepted);
  assert.deepEqual(sink.publishLifecycle(aborted), accepted);
  assert.deepEqual(sink.publishLifecycle({ ...aborted, eventId: 'recording-1:3' }), { status: 'duplicate' });
  assert.equal(await sink.drain(1000), true);
  assert.deepEqual(
    delivered.map(({ type }) => type),
    ['meeting.started', 'meeting.aborted']
  );
});

test('bounds closed lifecycle history and evicts the oldest recording deterministically', async () => {
  const sink = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 8, 2, 2);
  const started = (recordingId: string): MeetingStartedLifecycleEvent => ({
    ...event,
    eventId: `${recordingId}:1`,
    recordingId
  });
  const ended = (recordingId: string): MeetingTerminalLifecycleEvent => ({
    ...terminalEvent,
    eventId: `${recordingId}:2`,
    recordingId
  });

  for (const recordingId of ['recording-1', 'recording-2', 'recording-3']) {
    assert.deepEqual(sink.publishLifecycle(started(recordingId)), accepted);
    assert.deepEqual(sink.publishLifecycle(ended(recordingId)), accepted);
    assert.equal(await sink.drain(1000), true);
  }

  assert.deepEqual(sink.publishLifecycle(ended('recording-2')), { status: 'duplicate' });
  assert.deepEqual(sink.publishLifecycle(ended('recording-1')), { status: 'missing-start' });
  assert.deepEqual(sink.publishLifecycle(started('recording-1')), accepted);
  assert.deepEqual(sink.publishLifecycle(ended('recording-1')), accepted);
  assert.equal(await sink.drain(1000), true);
});

test('reports lifecycle admission telemetry by outcome classification', () => {
  const messages: Array<{ level: string; message: string }> = [];
  const telemetryLogger: MeetingIntegrationLogger = {
    debug: (message) => messages.push({ level: 'debug', message }),
    error: (message) => messages.push({ level: 'error', message }),
    warn: (message) => messages.push({ level: 'warn', message })
  };

  reportMeetingLifecyclePublishOutcome(telemetryLogger, event.recordingId, event.type, accepted);
  reportMeetingLifecyclePublishOutcome(telemetryLogger, event.recordingId, event.type, { status: 'capacity-exhausted' });
  reportMeetingLifecyclePublishOutcome(telemetryLogger, event.recordingId, 'meeting.aborted', { status: 'missing-start' });
  reportMeetingLifecyclePublishOutcome(telemetryLogger, event.recordingId, 'meeting.ended', { status: 'duplicate' });

  assert.deepEqual(
    messages.map(({ level }) => level),
    ['error', 'warn', 'debug']
  );
  assert.match(messages[0]?.message ?? '', /queue is full/);
  assert.match(messages[1]?.message ?? '', /without an accepted start/);
  assert.match(messages[2]?.message ?? '', /duplicate/);
});

test('tracks interleaved recording lifecycles independently', async () => {
  const transport: MeetingIntegrationTransport = {
    post: async () => undefined
  };
  const sink = new BoundedMeetingIntegrationSink(transport, logger, 8, 2);
  const secondStart: MeetingLifecycleEvent = {
    ...event,
    eventId: 'recording-2:1',
    recordingId: 'recording-2'
  };

  assert.deepEqual(sink.publishLifecycle(event), accepted);
  assert.deepEqual(sink.publishLifecycle(secondStart), accepted);
  assert.deepEqual(
    sink.publishLifecycle({
      ...event,
      eventId: 'recording-1:2',
      type: 'meeting.ended'
    }),
    accepted
  );
  assert.equal(sink.publishPacket({ ...packet, recordingId: 'recording-2' }, Buffer.from([1])), true);
  assert.equal(sink.publishPacket(packet, Buffer.from([2])), false);
  assert.deepEqual(
    sink.publishLifecycle({
      ...secondStart,
      eventId: 'recording-2:2',
      type: 'meeting.ended'
    }),
    accepted
  );
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

  assert.deepEqual(sink.publishLifecycle(event), accepted);
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
  sink.publishLifecycle({
    ...event,
    eventId: 'recording-1:2',
    type: 'meeting.ended'
  });

  assert.equal(await sink.drain(1000), true);
  assert.equal(attempts, 3);
  assert.deepEqual(delivered, ['/v1/craig/voice-packets', '/v1/craig/events']);
});

test('retries only the explicitly recoverable HTTP statuses', () => {
  for (const status of [408, 409, 425, 429, 500, 503, 599]) assert.equal(isRetryableMeetingIntegrationStatus(status), true, String(status));
  for (const status of [300, 400, 401, 403, 404, 410, 422, 499]) assert.equal(isRetryableMeetingIntegrationStatus(status), false, String(status));
});

test('fetches and validates the authenticated Meeting Platform channel snapshot', async (context) => {
  const requests: Array<{ path: string; headers: IncomingHttpHeaders }> = [];
  const server = createServer((request, response) => {
    requests.push({ path: request.url ?? '', headers: request.headers });
    response.writeHead(200, { 'content-type': 'application/json' }).end(
      JSON.stringify({
        schemaVersion: 1,
        channels: [
          { guildId: '1533228590643155035', voiceChannelId: '1533228823045214399' },
          { guildId: event.guildId, voiceChannelId: event.channelId }
        ]
      })
    );
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  context.after(async () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))));

  const address = server.address() as AddressInfo;
  const transport = new HttpMeetingIntegrationTransport(new URL(`http://127.0.0.1:${address.port}`), 'test-token', 1000);
  const configuration = await transport.getConfiguration();

  assert.equal(requests.length, 1);
  assert.equal(requests[0]?.path, '/v1/craig/configuration');
  assert.equal(requests[0]?.headers.authorization, 'Bearer test-token');
  assert.deepEqual(configuration, {
    schemaVersion: 1,
    channels: [
      { guildId: event.guildId, voiceChannelId: event.channelId },
      { guildId: '1533228590643155035', voiceChannelId: '1533228823045214399' }
    ]
  });
});

test('rejects malformed Meeting Platform snapshots before they can replace the active configuration', () => {
  assert.throws(
    () =>
      parseMeetingPlatformConfiguration({
        schemaVersion: 1,
        channels: [
          { guildId: event.guildId, voiceChannelId: event.channelId },
          { guildId: event.guildId, voiceChannelId: event.channelId }
        ]
      }),
    /duplicate/
  );
  assert.throws(() => parseMeetingPlatformConfiguration({ schemaVersion: 2, channels: [] }), /malformed/);
});

test('HTTP original recording contract streams audio metadata and requires ready 202', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-original-http-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const audioFilePath = path.join(root, 'track.ogg');
  const audio = Buffer.from('OggS-authoritative-track');
  await writeFile(audioFilePath, audio);
  let readyStatus = 200;
  let uploadAcknowledgementOverride: unknown;
  const requests: Array<{
    path: string;
    headers: IncomingHttpHeaders;
    body: Buffer;
  }> = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    requests.push({
      path: request.url ?? '',
      headers: request.headers,
      body: Buffer.concat(chunks)
    });
    if (request.url === '/v1/craig/authoritative-tracks') {
      const metadata = JSON.parse(Buffer.from(String(request.headers['x-craig-authoritative-track-metadata']), 'base64url').toString('utf8'));
      response.writeHead(202, { 'content-type': 'application/json' }).end(JSON.stringify(uploadAcknowledgementOverride ?? uploadAck(metadata)));
    } else response.writeHead(readyStatus).end();
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

  uploadAcknowledgementOverride = {};
  await assert.rejects(transport.postAuthoritativeTrack(metadata, audioFilePath), /acknowledgement is malformed/);
  uploadAcknowledgementOverride = { ...uploadAck(metadata), checksumSha256: '0'.repeat(64) };
  await assert.rejects(transport.postAuthoritativeTrack(metadata, audioFilePath), /does not match uploaded bytes/);
  uploadAcknowledgementOverride = undefined;

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

test('recovers the authoritative original after restart loses an incomplete live tee', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-original-outbox-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  const sourceFileBase = path.join(recordingRoot, 'recording-1.ogg');
  await mkdir(recordingRoot, { recursive: true });
  const sources: Record<string, string | Buffer> = {
    data: Buffer.concat([
      rawOggPage(900_000, 2, 1, Buffer.alloc(0)),
      rawOggPage(57_624, 1, 2, Buffer.from([0xf8, 0xff, 0xfe])),
      rawOggPage(123_456, 1, 3, Buffer.alloc(0)),
      rawOggPage(134_424, 2, 2, Buffer.from([0xf8, 0xff, 0xfe]))
    ]),
    header1: Buffer.from('original-header-1'),
    header2: Buffer.from('original-header-2'),
    users: '"0":{}\n,"1":{"id":"1533227577286852649"}\n,"2":{"id":"1533228054724346087","username":"Botik","globalName":"Botik","bot":true}\n',
    info: '{"format":1,"clientId":"1533228054724346087"}',
    log: 'closed\n'
  };
  await Promise.all(Object.entries(sources).map(([kind, contents]) => writeFile(`${sourceFileBase}.${kind}`, contents)));

  const liveDelivery: string[] = [];
  const activeBeforeRestart = new BoundedMeetingIntegrationSink(
    {
      async post(requestPath) {
        liveDelivery.push(requestPath);
        if (requestPath === '/v1/craig/voice-packets') throw new MeetingIntegrationDeliveryError('live tee permanently unavailable', false, 400);
      }
    },
    logger,
    1,
    1
  );
  assert.deepEqual(activeBeforeRestart.publishLifecycle(event), accepted);
  assert.equal(activeBeforeRestart.publishPacket(packet, Buffer.from([1, 2, 3])), true);
  assert.equal(await activeBeforeRestart.drain(1000), true);
  assert.deepEqual(liveDelivery, ['/v1/craig/events', '/v1/craig/voice-packets']);

  const firstAfterRestart = new BoundedMeetingIntegrationSink(
    { post: async () => undefined },
    logger,
    4,
    2,
    1024,
    { recordingRoot, outboxRoot },
    lifecycleV3Config
  );
  assert.equal(
    await firstAfterRestart.recoverInterruptedOriginalRecording({
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      startedAt: event.occurredAt,
      recoveredAt: terminalEvent.occurredAt,
      sourceFileBase
    }),
    true
  );
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending')), ['recording-1.json']);
  const pendingJobPath = path.join(outboxRoot, 'pending', 'recording-1.json');
  const recoveredJob = JSON.parse(await readFile(pendingJobPath, 'utf8')) as {
    schemaVersion: number;
    startedEvent: MeetingStartedLifecycleEvent;
    terminalEvent: MeetingTerminalLifecycleEvent;
    sourceFiles: Array<{ kind: string; relativePath: string }>;
    authoritativeTracks?: Array<Pick<AuthoritativeTrackMetadata, 'speakerId' | 'trackNumber' | 'timelineOffsetMs'>>;
  };
  assert.equal(recoveredJob.schemaVersion, 2, 'raw track identities must not be promoted to trusted lifecycle v3 evidence');
  assert.deepEqual(recoveredJob.startedEvent.participantIds, ['1533227577286852649']);
  assert.equal(recoveredJob.terminalEvent.type, 'meeting.ended');
  assert.match(recoveredJob.terminalEvent.reason ?? '', /restarted during an active recording/);
  recoveredJob.sourceFiles = recoveredJob.sourceFiles.map((source) => {
    const contents = sources[source.kind]!;
    const bytes = Buffer.isBuffer(contents) ? contents : Buffer.from(contents);
    return {
      ...source,
      checksumSha256: createHash('sha256').update(bytes).digest('hex'),
      sizeBytes: bytes.byteLength
    };
  });
  recoveredJob.authoritativeTracks = [
    { speakerId: '1533227577286852649', trackNumber: 1, timelineOffsetMs: 1200 },
    { speakerId: '1533228054724346087', trackNumber: 2, timelineOffsetMs: 2800 }
  ];
  await writeFile(pendingJobPath, `${JSON.stringify(recoveredJob)}\n`);

  const uploads: AuthoritativeTrackMetadata[] = [];
  const readyEvents: AuthoritativeRecordingReadyEvent[] = [];
  const lifecycleEvents: MeetingLifecycleEvent[] = [];
  const deliveryOrder: string[] = [];
  const preparedJobs: Array<{
    authoritativeTracks?: Array<Pick<AuthoritativeTrackMetadata, 'speakerId' | 'trackNumber' | 'timelineOffsetMs'>>;
    authoritativeTimelineBasis?: string;
  }> = [];
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
      return uploadAck(metadata);
    },
    async postAuthoritativeReady(ready) {
      readyAttempts++;
      deliveryOrder.push('ready');
      preparedJobs.push(JSON.parse(await readFile(path.join(outboxRoot, 'pending', 'recording-1.json'), 'utf8')) as typeof preparedJobs[number]);
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
  assert.deepEqual(lifecycleEvents, [recoveredJob.startedEvent, recoveredJob.terminalEvent, recoveredJob.startedEvent, recoveredJob.terminalEvent]);
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
    uploads.map(({ speakerId, trackNumber, timelineOffsetMs }) => ({
      speakerId,
      trackNumber,
      timelineOffsetMs
    })),
    [
      {
        speakerId: '1533227577286852649',
        trackNumber: 1,
        timelineOffsetMs: 1200
      },
      {
        speakerId: '1533228054724346087',
        trackNumber: 2,
        timelineOffsetMs: 1200
      },
      {
        speakerId: '1533227577286852649',
        trackNumber: 1,
        timelineOffsetMs: 1200
      },
      {
        speakerId: '1533228054724346087',
        trackNumber: 2,
        timelineOffsetMs: 1200
      }
    ]
  );
  assert.deepEqual(preparedJobs[0]?.authoritativeTracks, [
    {
      speakerId: '1533227577286852649',
      trackNumber: 1,
      timelineOffsetMs: 1200
    },
    {
      speakerId: '1533228054724346087',
      trackNumber: 2,
      timelineOffsetMs: 1200
    }
  ]);
  assert.equal(preparedJobs[0]?.authoritativeTimelineBasis, 'craig-cook-shared-origin-v1');
  assert.deepEqual(preparedJobs[1]?.authoritativeTracks, preparedJobs[0]?.authoritativeTracks);
  assert.deepEqual(
    uploads.slice(0, 2).map(({ trackNumber, timelineOffsetMs }) => timelineOffsetMs + (trackNumber === 1 ? 0 : 1600)),
    [1200, 2800]
  );
  assert.equal(new Set(uploads.map(({ uploadId }) => uploadId)).size, 2);
  assert.equal(readyEvents[0]?.trackCount, 2);
  assert.match(readyEvents[0]?.sourceFilesChecksumSha256 ?? '', /^[0-9a-f]{64}$/);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending')), []);
  for (const [kind, contents] of Object.entries(sources))
    assert.deepEqual(await readFile(`${sourceFileBase}.${kind}`), Buffer.isBuffer(contents) ? contents : Buffer.from(contents));
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
        data: rawOggPage(48_000, 1, 2, Buffer.from([0xf8, 0xff, 0xfe])),
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
  assert.equal(
    await staging.publishOriginalRecording({
      startedEvent: event,
      terminalEvent,
      sourceFileBase: corruptBase
    }),
    true
  );
  assert.equal(
    await staging.publishOriginalRecording({
      startedEvent: secondStarted,
      terminalEvent: secondTerminal,
      sourceFileBase: validBase
    }),
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
        return uploadAck(metadata);
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
  assert.deepEqual(await readFile(`${corruptBase}.data`), rawOggPage(48_000, 1, 2, Buffer.from([0xf8, 0xff, 0xfe])));
});

test('permanently rejects malformed and truncated raw Ogg data without touching originals', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-invalid-ogg-outbox-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  await mkdir(recordingRoot, { recursive: true });

  const malformed = rawOggPage(48_000, 1, 2, Buffer.from([0xf8, 0xff, 0xfe]));
  malformed[22] ^= 0xff;
  const complete = rawOggPage(96_000, 1, 2, Buffer.from([0xf8, 0xff, 0xfe]));
  const invalidRecordings = [
    { recordingId: 'recording-bad-crc', data: malformed },
    {
      recordingId: 'recording-truncated',
      data: complete.subarray(0, complete.byteLength - 1)
    }
  ];

  const staging = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, {
    recordingRoot,
    outboxRoot
  });
  for (const [index, invalid] of invalidRecordings.entries()) {
    const sourceFileBase = path.join(recordingRoot, `${invalid.recordingId}.ogg`);
    await Promise.all(
      Object.entries({
        data: invalid.data,
        header1: Buffer.from('original-header-1'),
        header2: Buffer.from('original-header-2'),
        users: '"0":{}\n,"1":{"id":"1533227577286852649"}\n',
        info: '{"format":1}',
        log: 'closed\n'
      }).map(([kind, contents]) => writeFile(`${sourceFileBase}.${kind}`, contents))
    );
    assert.equal(
      await staging.publishOriginalRecording({
        startedEvent: {
          ...event,
          eventId: `${invalid.recordingId}:1`,
          recordingId: invalid.recordingId,
          occurredAt: `2026-08-02T00:0${index + 2}:00.000Z`
        },
        terminalEvent: {
          ...terminalEvent,
          eventId: `${invalid.recordingId}:2`,
          recordingId: invalid.recordingId,
          occurredAt: `2026-08-02T00:0${index + 2}:30.000Z`
        },
        sourceFileBase
      }),
      true
    );
  }

  let cookCalls = 0;
  const restored = new BoundedMeetingIntegrationSink(
    {
      post: async () => undefined,
      postAuthoritativeTrack: async (metadata) => uploadAck(metadata),
      postAuthoritativeReady: async () => undefined
    },
    logger,
    4,
    2,
    1024,
    {
      recordingRoot,
      outboxRoot,
      cooker: {
        async cook() {
          cookCalls++;
          throw new Error('invalid raw data must be rejected before cooking');
        }
      }
    }
  );
  await restored.restoreOriginalRecordingJobs();

  assert.equal(await restored.drain(2000), true);
  assert.equal(cookCalls, 0);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending')), []);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'rejected')), ['recording-bad-crc.json', 'recording-truncated.json']);
  for (const invalid of invalidRecordings)
    assert.deepEqual(await readFile(path.join(recordingRoot, `${invalid.recordingId}.ogg.data`)), invalid.data);
});

test('publishes one aborted terminal lifecycle when recording finalization fails', async () => {
  const terminal = new MeetingTerminalLifecycle();
  const published: string[] = [];
  terminal.acceptStart(accepted);

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
  terminal.acceptStart(accepted);

  await terminal.complete(
    'meeting.ended',
    async () => undefined,
    (type) => published.push(type)
  );
  terminal.abort((type) => published.push(type));

  assert.deepEqual(published, ['meeting.ended']);
});

test('does not publish a phantom abort before a meeting start was accepted', () => {
  const terminal = new MeetingTerminalLifecycle();
  const published: string[] = [];

  assert.equal(terminal.acceptStart({ status: 'capacity-exhausted' }), false);
  terminal.abort((type) => published.push(type));

  assert.deepEqual(published, []);
});

test('publishes one abort after an accepted meeting start', () => {
  const terminal = new MeetingTerminalLifecycle();
  const published: string[] = [];
  assert.equal(terminal.acceptStart(accepted), true);

  terminal.abort((type) => published.push(type));
  terminal.abort((type) => published.push(type));

  assert.deepEqual(published, ['meeting.aborted']);
});

test('keeps lifecycle v1 compatibility by default and admits v3 only behind the explicit producer capability', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-admission-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  await mkdir(recordingRoot, { recursive: true });
  const delivered: MeetingLifecycleEvent[] = [];
  const legacy = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2);
  assert.deepEqual(legacy.lifecycleProducerConfiguration, { schemaVersion: 1 });

  const sink = new BoundedMeetingIntegrationSink(
    {
      post: async (requestPath, body) => {
        if (requestPath === '/v1/craig/events') delivered.push(body as MeetingLifecycleEvent);
      }
    },
    logger,
    4,
    2,
    1024,
    { recordingRoot, outboxRoot: path.join(root, 'outbox') },
    lifecycleV3Config
  );
  assert.deepEqual(sink.lifecycleProducerConfiguration, lifecycleV3Config);
  const lifecycle = createCraigLifecycleV3Producer(lifecycleV3Config, {
    recordingId: event.recordingId,
    guildId: event.guildId,
    channelId: event.channelId
  });
  const started = lifecycle.started(
    { eventId: 'recording-1:v3:1', recordingId: event.recordingId, guildId: event.guildId, channelId: event.channelId, occurredAt: event.occurredAt },
    [{ id: event.participantIds[0], bot: false, system: false, webhook: false }]
  );
  assert.equal(sink.publishLifecycle(started, lifecycle.durableSnapshot()).status, 'accepted');
  const terminal = lifecycle.terminal(
    {
      eventId: 'recording-1:v3:2',
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      occurredAt: terminalEvent.occurredAt
    },
    'meeting.ended',
    null
  );
  assert.equal(sink.publishLifecycle(terminal, lifecycle.durableSnapshot()).status, 'accepted');
  assert.equal(await sink.drain(1000), true);
  assert.deepEqual(delivered, [started, terminal]);
});

test('persists lifecycle v3 context, producer, event order, and pending outbox across restart', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-outbox-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  await mkdir(recordingRoot, { recursive: true });
  const lifecycle = createCraigLifecycleV3Producer(lifecycleV3Config, {
    recordingId: event.recordingId,
    guildId: event.guildId,
    channelId: event.channelId
  });
  const startedEvent = lifecycle.started(
    { eventId: 'recording-1:v3:1', recordingId: event.recordingId, guildId: event.guildId, channelId: event.channelId, occurredAt: event.occurredAt },
    [{ id: event.participantIds[0], bot: false, system: false, webhook: false }]
  );
  const endedEvent = lifecycle.terminal(
    {
      eventId: 'recording-1:v3:2',
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      occurredAt: terminalEvent.occurredAt
    },
    'meeting.ended',
    null
  );
  const staging = new BoundedMeetingIntegrationSink(
    { post: async () => undefined },
    logger,
    4,
    2,
    1024,
    {
      recordingRoot,
      outboxRoot
    },
    lifecycleV3Config
  );
  assert.equal(
    await staging.publishOriginalRecording({
      startedEvent,
      terminalEvent: endedEvent,
      sourceFileBase: path.join(recordingRoot, `${event.recordingId}.ogg`),
      lifecycleV3Snapshot: lifecycle.durableSnapshot()
    }),
    true
  );
  const persisted = JSON.parse(await readFile(path.join(outboxRoot, 'pending', `${event.recordingId}.json`), 'utf8'));
  assert.equal(persisted.schemaVersion, 3);
  assert.deepEqual(
    persisted.lifecycleV3Snapshot.emitted,
    [startedEvent, endedEvent].map(({ eventId, occurredAt }) => ({ eventId, occurredAt }))
  );
  assert.deepEqual(persisted.lifecycleV3Snapshot.pendingOutbox, [startedEvent, endedEvent]);

  const restored = new BoundedMeetingIntegrationSink(
    { post: async () => undefined },
    logger,
    4,
    2,
    1024,
    {
      recordingRoot,
      outboxRoot
    },
    lifecycleV3Config
  );
  await restored.restoreOriginalRecordingJobs();
  assert.equal(await restored.drain(0), false);
});

test('replays the exact ordered lifecycle v3 outbox and sealed ready event after a crash', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-replay-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const pendingRoot = path.join(root, 'outbox', 'pending');
  await Promise.all([mkdir(recordingRoot, { recursive: true }), mkdir(pendingRoot, { recursive: true })]);
  const lifecycle = createCraigLifecycleV3Producer(lifecycleV3Config, {
    recordingId: event.recordingId,
    guildId: event.guildId,
    channelId: event.channelId
  });
  const envelope = (eventId: string, occurredAt: string) => ({
    eventId,
    recordingId: event.recordingId,
    guildId: event.guildId,
    channelId: event.channelId,
    occurredAt
  });
  const started = lifecycle.started(envelope('recording-1:v3:1', event.occurredAt), [
    { id: event.participantIds[0], bot: false, system: false, webhook: false }
  ]);
  const joined = lifecycle.participant(envelope('recording-1:v3:2', '2026-08-02T00:00:30.000Z'), 'participant.joined', {
    id: '1533228054724346087',
    bot: true,
    system: false,
    webhook: false
  });
  const ended = lifecycle.terminal(envelope('recording-1:v3:3', terminalEvent.occurredAt), 'meeting.ended', null);
  const sourceFiles = (['data', 'header1', 'header2', 'users', 'info', 'log'] as const).map((kind) => ({
    kind,
    relativePath: `${event.recordingId}.ogg.${kind}`,
    checksumSha256: createHash('sha256').update(kind).digest('hex'),
    sizeBytes: kind.length
  }));
  const sourceFilesChecksumSha256 = createHash('sha256').update(JSON.stringify(sourceFiles), 'utf8').digest('hex');
  const ready = lifecycle.authoritativeReady(envelope('recording-1:authoritative-ready:v3', terminalEvent.occurredAt), {
    actors: [
      { id: event.participantIds[0], bot: false, system: false, webhook: false },
      { id: '1533228054724346087', bot: true, system: false, webhook: false }
    ],
    endedAt: terminalEvent.occurredAt,
    trackCount: 1,
    sourceFilesChecksumSha256
  });
  await writeFile(
    path.join(pendingRoot, `${event.recordingId}.json`),
    `${JSON.stringify({
      schemaVersion: 3,
      publicationId: `authoritative-recording:v3:${event.recordingId}`,
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      startedEvent: started,
      terminalEvent: ended,
      sourceFiles,
      authoritativeTracks: [{ speakerId: event.participantIds[0], trackNumber: 1, timelineOffsetMs: 0 }],
      authoritativeTimelineBasis: 'craig-cook-shared-origin-v1',
      lifecycleV3Snapshot: lifecycle.durableSnapshot()
    })}\n`
  );

  const syntheticTrack = Buffer.from('OggS-synthetic');
  await writeFile(path.join(root, 'synthetic.ogg'), syntheticTrack);
  const delivered: MeetingLifecycleEvent[] = [];
  const readyEvents: AuthoritativeRecordingReadyEvent[] = [];
  const restored = new BoundedMeetingIntegrationSink(
    {
      post: async (requestPath, body) => {
        if (requestPath === '/v1/craig/events') delivered.push(body as MeetingLifecycleEvent);
      },
      postAuthoritativeTrack: async (metadata) => uploadAck(metadata),
      postAuthoritativeReady: async (readyEvent) => {
        readyEvents.push(readyEvent);
      }
    },
    logger,
    4,
    2,
    1024,
    {
      recordingRoot,
      outboxRoot: path.join(root, 'outbox'),
      cooker: {
        async cook() {
          return {
            filePath: path.join(root, 'synthetic.ogg'),
            checksumSha256: createHash('sha256').update(syntheticTrack).digest('hex'),
            sizeBytes: syntheticTrack.length,
            async dispose() {}
          };
        }
      }
    },
    lifecycleV3Config
  );
  await restored.restoreOriginalRecordingJobs();
  assert.equal(await restored.drain(2000), true);
  assert.deepEqual(delivered, [started, joined, ended]);
  assert.deepEqual(readyEvents, [ready]);
  assert.deepEqual(await readdir(pendingRoot), []);
});

test('hard-crash recovery replays exact durable v3 identities and never fabricates actor evidence', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-hard-crash-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  const sourceFileBase = path.join(recordingRoot, `${event.recordingId}.ogg`);
  await mkdir(recordingRoot, { recursive: true });
  const sources: Record<string, string | Buffer> = {
    data: Buffer.concat([rawOggPage(57_624, 1, 1, Buffer.from([0xf8, 0xff, 0xfe]))]),
    header1: Buffer.from('header-1'),
    header2: Buffer.from('header-2'),
    users: `"0":{}\n,"1":{"id":"${event.participantIds[0]}"}\n`,
    info: '{"format":1,"clientId":"1533228054724346087"}',
    log: 'interrupted\n'
  };
  await Promise.all(Object.entries(sources).map(([kind, contents]) => writeFile(`${sourceFileBase}.${kind}`, contents)));

  const lifecycle = createCraigLifecycleV3Producer(lifecycleV3Config, {
    recordingId: event.recordingId,
    guildId: event.guildId,
    channelId: event.channelId
  });
  const staging = new BoundedMeetingIntegrationSink(
    { post: async () => await new Promise<void>(() => {}) },
    logger,
    4,
    2,
    1024,
    { recordingRoot, outboxRoot },
    lifecycleV3Config
  );
  const started = lifecycle.started(
    {
      eventId: 'original:v3:start',
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      occurredAt: event.occurredAt
    },
    [{ id: event.participantIds[0], bot: false, system: false, webhook: false }]
  );
  assert.equal(staging.publishLifecycle(started, lifecycle.durableSnapshot()).status, 'accepted');
  const joined = lifecycle.participant(
    {
      eventId: 'original:v3:join',
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      occurredAt: '2026-08-02T00:00:30.000Z'
    },
    'participant.joined',
    { id: '1533228054724346087', bot: true, system: false, webhook: false }
  );
  assert.equal(staging.publishLifecycle(joined, lifecycle.durableSnapshot()).status, 'accepted');
  const leftOnlyActorId = '1533228054724346099';
  const left = lifecycle.participant(
    {
      eventId: 'original:v3:left', recordingId: event.recordingId, guildId: event.guildId,
      channelId: event.channelId, occurredAt: '2026-08-02T00:00:31.000Z'
    },
    'participant.left',
    { id: leftOnlyActorId, bot: false, system: false, webhook: false }
  );
  assert.equal(staging.publishLifecycle(left, lifecycle.durableSnapshot()).status, 'accepted');
  const conflictingJoin = lifecycle.participant(
    {
      eventId: 'original:v3:conflict', recordingId: event.recordingId, guildId: event.guildId,
      channelId: event.channelId, occurredAt: '2026-08-02T00:00:32.000Z'
    },
    'participant.joined',
    { id: leftOnlyActorId, bot: true, system: false, webhook: false }
  );
  assert.equal(staging.publishLifecycle(conflictingJoin, lifecycle.durableSnapshot()).status, 'accepted');

  const replayed: MeetingLifecycleEvent[] = [];
  const replay = new BoundedMeetingIntegrationSink(
    {
      post: async (requestPath, body) => {
        if (requestPath === '/v1/craig/events') replayed.push(body as MeetingLifecycleEvent);
      }
    },
    logger,
    4,
    2,
    1024,
    { recordingRoot, outboxRoot }
  );
  await replay.restoreOriginalRecordingJobs();
  assert.equal(await replay.drain(1000), true);
  assert.deepEqual(replayed, [started, joined, left, conflictingJoin]);
  assert.deepEqual(
    await readdir(path.join(outboxRoot, 'lifecycle-v3', 'pending', `${event.recordingId}.journal`)),
    [
      '00000000.event.json', '00000001.event.json', '00000002.event.json', '00000003.event.json',
      'cursor.json', 'manifest.json', 'manifest.previous.json', 'snapshot-00000000.json'
    ],
    'the fsynced ack cursor pins its exact segment while compacting older acknowledged events'
  );
  const manifest = JSON.parse(
    await readFile(path.join(outboxRoot, 'lifecycle-v3', 'pending', `${event.recordingId}.journal`, 'manifest.json'), 'utf8')
  );
  assert.equal(manifest.current.generation, 0);
  const initialFallback = JSON.parse(
    await readFile(path.join(outboxRoot, 'lifecycle-v3', 'pending', `${event.recordingId}.journal`, 'manifest.previous.json'), 'utf8')
  );
  assert.deepEqual(initialFallback, manifest, 'initial current publication has an independently fsynced valid fallback');
  const cursorPath = path.join(outboxRoot, 'lifecycle-v3', 'pending', `${event.recordingId}.journal`, 'cursor.json');
  const compactedCursor = JSON.parse(await readFile(cursorPath, 'utf8'));
  assert.equal(compactedCursor.ackedSequence, 3);

  await writeFile(cursorPath, `${JSON.stringify({ ...compactedCursor, digestSha256: '0'.repeat(64) })}\n`);
  const corruptRecovery = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, { recordingRoot, outboxRoot });
  assert.throws(() => (corruptRecovery as any).readLifecycleV3Snapshot(event.recordingId), /cursor does not match/);
  await writeFile(cursorPath, `${JSON.stringify(compactedCursor)}\n`);

  const manifestPath = path.join(outboxRoot, 'lifecycle-v3', 'pending', `${event.recordingId}.journal`, 'manifest.json');
  await writeFile(manifestPath, '{"schemaVersion":');
  const tornManifestRecovery = new BoundedMeetingIntegrationSink(
    { post: async () => undefined }, logger, 4, 2, 1024, { recordingRoot, outboxRoot }
  );
  assert.equal((tornManifestRecovery as any).readLifecycleV3Snapshot(event.recordingId).recordingId, event.recordingId);
  assert.equal(JSON.parse(await readFile(manifestPath, 'utf8')).current.generation, 0);

  const recovery = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, {
    recordingRoot,
    outboxRoot
  });
  assert.equal(
    await recovery.recoverInterruptedOriginalRecording({
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      startedAt: event.occurredAt,
      recoveredAt: terminalEvent.occurredAt,
      sourceFileBase
    }),
    true
  );
  const recovered = JSON.parse(await readFile(path.join(outboxRoot, 'pending', `${event.recordingId}.json`), 'utf8'));
  assert.deepEqual(
    recovered.lifecycleV3Snapshot.pendingOutbox,
    [started, recovered.terminalEvent],
    'acked compacted events are not replayed, while the pinned trusted start remains available for crash recovery'
  );
  assert.equal(recovered.startedEvent.eventId, started.eventId);
  assert.deepEqual(recovered.lifecycleV3Snapshot.actors, [
    { actorId: event.participantIds[0], kind: 'human' },
    { actorId: '1533228054724346087', kind: 'automation' },
    { actorId: leftOnlyActorId, kind: 'human' }
  ]);
  assert.equal(recovered.lifecycleV3Snapshot.actorObservationState, 'conflicted');
});

test('restores pending v3 jobs using their immutable producer revision after a rolling revision', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-rolling-revision-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  await mkdir(recordingRoot, { recursive: true });
  const lifecycle = createCraigLifecycleV3Producer(lifecycleV3Config, {
    recordingId: event.recordingId,
    guildId: event.guildId,
    channelId: event.channelId
  });
  const started = lifecycle.started(
    { eventId: 'rolling:v3:start', recordingId: event.recordingId, guildId: event.guildId, channelId: event.channelId, occurredAt: event.occurredAt },
    []
  );
  const aborted = lifecycle.terminal(
    {
      eventId: 'rolling:v3:abort',
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      occurredAt: terminalEvent.occurredAt
    },
    'meeting.aborted',
    'crash'
  );
  const staging = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, { recordingRoot, outboxRoot });
  assert.equal(
    await staging.publishOriginalRecording({
      startedEvent: started,
      terminalEvent: aborted,
      lifecycleV3Snapshot: lifecycle.durableSnapshot(),
      sourceFileBase: path.join(recordingRoot, `${event.recordingId}.ogg`)
    }),
    true
  );

  const delivered: MeetingLifecycleEvent[] = [];
  const changedRevision = { ...lifecycleV3Config, producerRevision: 'f'.repeat(40) };
  const restored = new BoundedMeetingIntegrationSink(
    {
      post: async (_requestPath, body) => {
        delivered.push(body as MeetingLifecycleEvent);
      },
      postAuthoritativeTrack: async (metadata) => uploadAck(metadata),
      postAuthoritativeReady: async () => undefined
    },
    logger,
    4,
    2,
    1024,
    { recordingRoot, outboxRoot, cooker: { cook: async () => assert.fail('aborted recording must not be cooked') } },
    changedRevision
  );
  await restored.restoreOriginalRecordingJobs();
  assert.equal(await restored.drain(1000), true);
  assert.deepEqual(delivered, [started, aborted]);
});

test('restores pending v3 jobs while current admission has rolled back to v1', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-to-v1-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  await mkdir(recordingRoot, { recursive: true });
  const lifecycle = createCraigLifecycleV3Producer(lifecycleV3Config, {
    recordingId: event.recordingId,
    guildId: event.guildId,
    channelId: event.channelId
  });
  const started = lifecycle.started(
    {
      eventId: 'rollback:v3:start',
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      occurredAt: event.occurredAt
    },
    []
  );
  const aborted = lifecycle.terminal(
    {
      eventId: 'rollback:v3:abort',
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId,
      occurredAt: terminalEvent.occurredAt
    },
    'meeting.aborted',
    'rollback'
  );
  const staging = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, { recordingRoot, outboxRoot });
  assert.equal(
    await staging.publishOriginalRecording({
      startedEvent: started,
      terminalEvent: aborted,
      lifecycleV3Snapshot: lifecycle.durableSnapshot(),
      sourceFileBase: path.join(recordingRoot, `${event.recordingId}.ogg`)
    }),
    true
  );
  const delivered: MeetingLifecycleEvent[] = [];
  const restored = new BoundedMeetingIntegrationSink(
    {
      post: async (_requestPath, body) => {
        delivered.push(body as MeetingLifecycleEvent);
      },
      postAuthoritativeTrack: async (metadata) => uploadAck(metadata),
      postAuthoritativeReady: async () => undefined
    },
    logger,
    4,
    2,
    1024,
    { recordingRoot, outboxRoot, cooker: { cook: async () => assert.fail('aborted recording must not be cooked') } }
  );
  await restored.restoreOriginalRecordingJobs();
  assert.equal(await restored.drain(1000), true);
  assert.deepEqual(delivered, [started, aborted]);
});

test('poisons duplicate original admission when identical IDs hide a conflicting v3 snapshot', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-conflicting-admission-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  await mkdir(recordingRoot, { recursive: true });
  const createAdmission = (bot: boolean) => {
    const lifecycle = createCraigLifecycleV3Producer(lifecycleV3Config, {
      recordingId: event.recordingId,
      guildId: event.guildId,
      channelId: event.channelId
    });
    const startedEvent = lifecycle.started(
      { eventId: 'same:v3:start', recordingId: event.recordingId, guildId: event.guildId, channelId: event.channelId, occurredAt: event.occurredAt },
      [{ id: event.participantIds[0], bot, system: false, webhook: false }]
    );
    const terminalEvent = lifecycle.terminal(
      {
        eventId: 'same:v3:abort',
        recordingId: event.recordingId,
        guildId: event.guildId,
        channelId: event.channelId,
        occurredAt: terminalEventTime
      },
      'meeting.aborted',
      'same ids'
    );
    return {
      startedEvent,
      terminalEvent,
      lifecycleV3Snapshot: lifecycle.durableSnapshot(),
      sourceFileBase: path.join(recordingRoot, `${event.recordingId}.ogg`)
    };
  };
  const terminalEventTime = terminalEvent.occurredAt;
  const sink = new BoundedMeetingIntegrationSink({ post: async () => undefined }, logger, 4, 2, 1024, { recordingRoot, outboxRoot });
  assert.equal(await sink.publishOriginalRecording(createAdmission(false)), true);
  assert.equal(await sink.publishOriginalRecording(createAdmission(true)), false);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending')), []);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'rejected')), [`${event.recordingId}.json`]);
});

test('production original upload durably manifests and emits the exact trusted cancellation proof', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-cancellation-proof-production-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  const sourceFileBase = path.join(recordingRoot, `${event.recordingId}.ogg`);
  const botId = '1533228054724346087';
  await mkdir(recordingRoot, { recursive: true });
  await Promise.all([
    writeFile(`${sourceFileBase}.data`, rawOggPage(57_624, 1, 1, Buffer.from([0xf8, 0xff, 0xfe]))),
    writeFile(`${sourceFileBase}.header1`, 'header-1'),
    writeFile(`${sourceFileBase}.header2`, 'header-2'),
    writeFile(`${sourceFileBase}.users`, `"0":{}\n,"1":{"id":"${botId}","bot":true}\n`),
    writeFile(`${sourceFileBase}.info`, `{"format":1,"clientId":"${botId}"}`),
    writeFile(`${sourceFileBase}.log`, 'closed\n'),
    writeFile(
      `${sourceFileBase}.playback-cancellation-fence.trusted.json`,
      `${JSON.stringify({
        schemaVersion: 2,
        type: 'playback-cancel',
        meetingId: 'meeting-trusted-1',
        recordingId: event.recordingId,
        turnId: 'turn-1',
        attemptId: 'attempt-1',
        cancellationObservedAtMs: Date.parse('2026-08-18T00:00:03.000Z'),
        reason: 'barge-in',
        playbackGeneration: 1,
        attemptGenerationToken: 'attempt-generation-token-1',
        postCancellationAttemptedPacketCount: 1,
        postCancellationAcceptedPacketCount: 0
      })}\n`
    ),
    writeFile(
      `${sourceFileBase}.playback-cancellation-fence.legacy.json`,
      '{"schemaVersion":1,"recordingId":"recording-1","turnId":"fabricated","attemptId":"fabricated"}\n'
    ),
    writeFile(
      `${sourceFileBase}.playback-cancellation-fence.expanded-year.json`,
      `${JSON.stringify({
        schemaVersion: 2, type: 'playback-cancel', meetingId: 'meeting-legacy-expanded-year',
        recordingId: event.recordingId, turnId: 'turn-expanded', attemptId: 'attempt-expanded',
        cancellationObservedAtMs: 253_402_300_800_000, reason: 'barge-in', playbackGeneration: 1,
        attemptGenerationToken: 'attempt-generation-token-expanded',
        postCancellationAttemptedPacketCount: 1, postCancellationAcceptedPacketCount: 0
      })}\n`
    )
  ]);
  const cookedBytes = Buffer.from('OggS-final-botik-track');
  const cookedPath = path.join(root, 'botik.ogg');
  await writeFile(cookedPath, cookedBytes);
  const checksumSha256 = createHash('sha256').update(cookedBytes).digest('hex');
  const proofs: unknown[] = [];
  const sink = new BoundedMeetingIntegrationSink(
    {
      post: async () => undefined,
      postAuthoritativeTrack: async (metadata) => uploadAck(metadata),
      postAuthoritativeReady: async () => undefined,
      postCancellationPcmFence: async (proof) => {
        proofs.push(proof);
        return proofReceipt(proof, uploadAck({
          schemaVersion: 1, uploadId: `authoritative-track:v1:${event.recordingId}:1`, recordingId: event.recordingId,
          guildId: event.guildId, channelId: event.channelId, trackNumber: 1, speakerId: botId, timelineOffsetMs: 0,
          checksumSha256, sizeBytes: cookedBytes.length
        }));
      }
    },
    logger,
    4,
    2,
    1024,
    {
      recordingRoot,
      outboxRoot,
      cooker: {
        cook: async () => ({ filePath: cookedPath, checksumSha256, sizeBytes: cookedBytes.length, async dispose() {} })
      }
    }
  );
  assert.equal(await sink.publishOriginalRecording({ startedEvent: event, terminalEvent, sourceFileBase }), true);
  assert.equal(await sink.drain(2000), true);
  assert.equal(proofs.length, 1, 'legacy or expanded-year sidecars are retained without blocking trusted proof publication');
  assert.deepEqual(proofs[0], {
    acceptedPacketCountAfterCancellation: 0,
    attemptedPacketCountAfterCancellation: 1,
    attemptId: 'attempt-1',
    cancellationObservedAt: '2026-08-18T00:00:03.000Z',
    fenceObservedAt: '2026-08-18T00:00:03.000Z',
    meetingId: 'meeting-trusted-1',
    message: 'Craig authoritative cancellation PCM fence observed',
    recordingId: event.recordingId,
    source: 'craig-authoritative-playback-track',
    trackSha256: checksumSha256,
    turnId: 'turn-1'
  });
  const manifestRoot = path.join(outboxRoot, 'pending', 'cancellation-pcm-fence-manifests');
  const [manifestName] = await readdir(manifestRoot);
  const manifest = JSON.parse(await readFile(path.join(manifestRoot, manifestName), 'utf8'));
  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.uploadId, `authoritative-track:v1:${event.recordingId}:1`);
  assert.equal(manifest.recordingId, event.recordingId);
  assert.equal(manifest.speakerId, botId);
  assert.equal(manifest.trackNumber, 1);
  assert.equal(manifest.trackSha256, checksumSha256);
  assert.equal(manifest.trackSizeBytes, cookedBytes.length);
  assert.deepEqual(manifest.proof, proofs[0]);
  assert.deepEqual(await readdir(path.join(outboxRoot, 'pending', 'cancellation-pcm-fence')), []);

  const proofRoot = path.join(outboxRoot, 'pending', 'cancellation-pcm-fence');
  await writeFile(path.join(proofRoot, manifestName), `${JSON.stringify(proofs[0])}\n`);
  const missingReceipt = new BoundedMeetingIntegrationSink(
    { post: async () => undefined, postCancellationPcmFence: async () => ({} as any) },
    logger, 4, 2, 1024, { recordingRoot, outboxRoot }
  );
  await assert.rejects(() => (missingReceipt as any).replayCancellationProofs(), /receipt is malformed/);
  assert.deepEqual(await readdir(proofRoot), [manifestName], 'missing receipt preserves the proof outbox');

  const mismatchedReceipt = new BoundedMeetingIntegrationSink(
    { post: async () => undefined, postCancellationPcmFence: async (proof) => ({
      ...proofReceipt(proof, manifest.uploadAcknowledgement), checksumSha256: '0'.repeat(64)
    }) },
    logger, 4, 2, 1024, { recordingRoot, outboxRoot }
  );
  await assert.rejects(() => (mismatchedReceipt as any).replayCancellationProofs(), /does not match/);
  assert.deepEqual(await readdir(proofRoot), [manifestName], 'mismatched receipt preserves the proof outbox');

  const manifestPath = path.join(manifestRoot, manifestName);
  await writeFile(manifestPath, `${JSON.stringify({
    ...manifest, uploadAcknowledgement: { ...manifest.uploadAcknowledgement, checksumSha256: '0'.repeat(64) }
  })}\n`);
  await assert.rejects(() => (mismatchedReceipt as any).replayCancellationProofs(), /manifest is corrupt or mismatched|does not match/);
  assert.deepEqual(await readdir(proofRoot), [manifestName], 'corrupt restart manifest fails closed');
  await writeFile(manifestPath, `${JSON.stringify(manifest)}\n`);
  const replayedProofs: unknown[] = [];
  const replay = new BoundedMeetingIntegrationSink(
    { post: async () => undefined, postCancellationPcmFence: async (proof) => {
      replayedProofs.push(proof);
      return proofReceipt(proof, manifest.uploadAcknowledgement);
    } },
    logger, 4, 2, 1024, { recordingRoot, outboxRoot }
  );
  await replay.restoreOriginalRecordingJobs();
  assert.equal(await replay.drain(2000), true);
  assert.deepEqual(replayedProofs, proofs);
  assert.deepEqual(await readdir(proofRoot), [], 'proof outbox is acknowledged only after successful crash replay');
});

test('cancellation proof retry is autonomous and does not block original outbox scanning', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-proof-retry-worker-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingRoot = path.join(root, 'recordings');
  const outboxRoot = path.join(root, 'outbox');
  const pendingRoot = path.join(outboxRoot, 'pending');
  const proofRoot = path.join(pendingRoot, 'cancellation-pcm-fence');
  const manifestRoot = path.join(pendingRoot, 'cancellation-pcm-fence-manifests');
  await Promise.all([mkdir(recordingRoot, { recursive: true }), mkdir(proofRoot, { recursive: true }), mkdir(manifestRoot, { recursive: true })]);
  const recordingId = 'proof-retry-recording';
  const speakerId = '1533228054724346087';
  const trackSha256 = 'a'.repeat(64);
  const proof = {
    acceptedPacketCountAfterCancellation: 0, attemptedPacketCountAfterCancellation: 1,
    attemptId: 'attempt-1', cancellationObservedAt: '2026-08-18T00:00:03.000Z',
    fenceObservedAt: '2026-08-18T00:00:03.000Z', meetingId: 'meeting-1',
    message: 'Craig authoritative cancellation PCM fence observed' as const, recordingId,
    source: 'craig-authoritative-playback-track' as const, trackSha256, turnId: 'turn-1'
  };
  const metadata = {
    schemaVersion: 1 as const, uploadId: `authoritative-track:v1:${recordingId}:1`, recordingId,
    guildId: event.guildId, channelId: event.channelId, speakerId, trackNumber: 1,
    timelineOffsetMs: 0, checksumSha256: trackSha256, sizeBytes: 123
  };
  const acknowledgement = uploadAck(metadata);
  const fileName = `${recordingId}.pcm-fence.retry.json`;
  const manifest = {
    schemaVersion: 1, uploadAcknowledgement: acknowledgement, uploadId: metadata.uploadId,
    recordingId, guildId: event.guildId, channelId: event.channelId, speakerId, trackNumber: 1,
    trackSha256, trackSizeBytes: metadata.sizeBytes, proof
  };
  await Promise.all([
    writeFile(path.join(proofRoot, fileName), `${JSON.stringify(proof)}\n`),
    writeFile(path.join(manifestRoot, fileName), `${JSON.stringify(manifest)}\n`),
    writeFile(path.join(pendingRoot, 'unrelated-invalid.json'), '{"schemaVersion":')
  ]);
  let attempts = 0;
  const sink = new BoundedMeetingIntegrationSink({
    post: async () => undefined,
    postCancellationPcmFence: async (delivered) => {
      attempts++;
      if (attempts === 1) throw new MeetingIntegrationDeliveryError('temporary proof outage', true, 503);
      return proofReceipt(delivered, acknowledgement);
    }
  }, logger, 4, 2, 1024, { recordingRoot, outboxRoot });
  await sink.restoreOriginalRecordingJobs();
  assert.deepEqual(await readdir(path.join(outboxRoot, 'rejected')), ['unrelated-invalid.json'],
    'proof retry does not abort the unrelated original-job scan');
  assert.equal(await sink.drain(2000), true, 'transient proof delivery recovers automatically without restart');
  assert.equal(attempts, 2);
  assert.deepEqual(await readdir(proofRoot), []);

  for (const [field, value] of [
    ['schemaVersion', 2], ['recordingId', 'different-recording'], ['speakerId', 'invalid'],
    ['trackNumber', 2], ['trackSha256', 'b'.repeat(64)], ['uploadId', 'authoritative-track:v1:wrong:1']
  ] as const) {
    await writeFile(path.join(proofRoot, fileName), `${JSON.stringify(proof)}\n`);
    await writeFile(path.join(manifestRoot, fileName), `${JSON.stringify({ ...manifest, [field]: value })}\n`);
    await assert.rejects(() => (sink as any).replayCancellationProofs(), /manifest is corrupt or mismatched/,
      `${field} restart identity mismatch fails closed`);
  }
  await writeFile(path.join(manifestRoot, fileName), `${JSON.stringify({ ...manifest, unexpected: true })}\n`);
  await assert.rejects(() => (sink as any).replayCancellationProofs(), /manifest is corrupt or mismatched/,
    'unknown manifest keys fail closed');
});

test('cancellation proof worker rotates a delayed selection window before sleeping', async () => {
  const sink = new BoundedMeetingIntegrationSink(
    { post: async () => undefined, postCancellationPcmFence: async () => { throw new Error('unused'); } },
    logger, 4, 2
  );
  const now = Date.now();
  const jobs = Array.from({ length: 9 }, (_, index) => ({
    filePath: `proof-${index}.json`, consecutiveFailures: 1,
    notBeforeMs: index < 8 ? now + 10_000 : 0
  }));
  (sink as any).cancellationProofJobs.push(...jobs);
  const delivered: string[] = [];
  (sink as any).deliverCancellationProof = async (filePath: string) => { delivered.push(filePath); };
  const scheduledDelays: number[] = [];
  (sink as any).scheduleCancellationProofProcessing = (delayMs = 0) => { scheduledDelays.push(delayMs); };

  await (sink as any).processCancellationProof();
  assert.deepEqual(delivered, []);
  assert.equal(scheduledDelays.shift(), 0, 'an uninspected window is scheduled immediately');
  await (sink as any).processCancellationProof();
  assert.deepEqual(delivered, ['proof-8.json'], 'ready proof behind eight delayed retries is not hidden by backoff');
});

test('lifecycle v3 COMPLETE and ACK hot paths have constant persistence for N=10 and N=5000', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-bounded-hot-path-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));

  const canonical = (value: unknown): string => {
    if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
    if (value !== null && typeof value === 'object') return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(',')}}`;
    return JSON.stringify(value) as string;
  };
  type Operation = 'ordinary-complete' | 'threshold-complete' | 'ordinary-ack' | 'threshold-ack';
  const measure = async (actorCount: number, operation: Operation) => {
    const recordingId = actorCount === 10 ? `hotpath-a-${operation}` : `hotpath-b-${operation}`;
    const outboxRoot = path.join(root, recordingId);
    const sink = new BoundedMeetingIntegrationSink(
      { post: async () => undefined }, logger, 4, 2, 6000,
      { recordingRoot: root, outboxRoot }, lifecycleV3Config
    );
    const actors = Array.from({ length: actorCount }, (_, index) => ({
      actorId: String(10_000_000_000_000_000n + BigInt(index % 1000)), kind: 'human' as const
    }));
    const uniqueActorCount = new Set(actors.map(({ actorId }) => actorId)).size;
    const sequence = operation.startsWith('threshold') ? 128 : 127;
    const lifecycleEvent = {
      schemaVersion: 3 as const, eventId: `event-${operation}`, recordingId,
      guildId: event.guildId, channelId: event.channelId,
      occurredAt: '2026-08-18T00:00:00.000Z', type: 'participant.joined' as const,
      actorSemanticsVersion, producerCapabilityId: sealedActorRosterCapabilityId,
      producerRevision: lifecycleV3Config.producerRevision, actorObservationState: 'consistent' as const,
      actor: { actorId: '19999999999999999', kind: 'human' as const }
    };
    const digest = createHash('sha256').update(canonical(lifecycleEvent)).digest('hex');
    const journalRoot = path.join(outboxRoot, 'lifecycle-v3', 'pending', `${recordingId}.journal`);
    await mkdir(journalRoot, { recursive: true });
    const rosterMaterializationTrap = new Proxy(actors, {
      get(target, property, receiver) {
        if (property === Symbol.iterator || property === 'length' || property === 'map' || property === 'filter' ||
            property === 'slice' || property === 'sort')
          throw new Error(`${operation} materialized the full actor roster`);
        return Reflect.get(target, property, receiver);
      }
    });
    const isAck = operation.endsWith('ack');
    const state = {
      snapshot: {
        schemaVersion: 2, recordingId, guildId: event.guildId, channelId: event.channelId,
        producer: lifecycleV3Config, actorObservationState: 'consistent', actors: rosterMaterializationTrap,
        sealedReady: null, emitted: [], pendingOutbox: []
      },
      actorIndex: new Map(actors.map((actor) => [actor.actorId, actor])),
      pendingEvents: isAck ? new Map([[sequence, lifecycleEvent]]) : new Map(),
      eventDigests: isAck ? new Map([[lifecycleEvent.eventId, digest]]) : new Map(),
      generation: 0, nextSequence: sequence + (isAck ? 1 : 0), ackedSequence: sequence - 1,
      closed: false, lastOccurredAt: lifecycleEvent.occurredAt,
      lastAcknowledgedEventId: null, lastAcknowledgedDigest: null, maintenanceNeeded: false
    };
    (sink as any).lifecycleV3JournalIndex.set(recordingId, state);
    let calls = 0;
    let bytes = 0;
    (sink as any).writeDurableJson = (_filePath: string, value: unknown) => {
      calls++;
      bytes += Buffer.byteLength(`${JSON.stringify(value)}\n`);
    };
    (sink as any).runLifecycleV3MaintenanceJournalStep = () => {
      throw new Error(`${operation} synchronously ran maintenance`);
    };
    if (isAck) {
      (sink as any).acknowledgeLifecycleV3Event(lifecycleEvent);
      assert.equal(state.pendingEvents.size, 0, `${operation} physically evicts the pending event`);
      assert.equal(state.eventDigests.size, 0, `${operation} physically evicts the event digest`);
      assert.equal(state.actorIndex.size, uniqueActorCount, `${operation} preserves the indexed roster`);
    } else {
      (sink as any).appendLifecycleV3Event(undefined, lifecycleEvent, sequence);
      assert.equal(state.pendingEvents.size, 1, `${operation} indexes one pending event`);
      assert.equal(state.eventDigests.size, 1, `${operation} indexes one event digest`);
      assert.equal(state.actorIndex.size, uniqueActorCount + 1, `${operation} applies the participant delta once`);
    }
    const threshold = sequence === 128 && isAck;
    assert.equal(state.maintenanceNeeded, threshold, `${operation} only marks the ACK maintenance boundary`);
    assert.equal((sink as any).lifecycleV3MaintenanceQueue.size, threshold ? 1 : 0);
    assert.equal((sink as any).lifecycleV3MaintenanceTimer !== null, threshold,
      `${operation} schedules maintenance only after the ACK boundary`);
    if ((sink as any).lifecycleV3MaintenanceTimer !== null) {
      clearTimeout((sink as any).lifecycleV3MaintenanceTimer);
      (sink as any).lifecycleV3MaintenanceTimer = null;
    }
    return { calls, bytes, pendingEvents: state.pendingEvents.size, eventDigests: state.eventDigests.size };
  };

  const evidence: Record<Operation, Awaited<ReturnType<typeof measure>>> = {} as any;
  for (const operation of ['ordinary-complete', 'threshold-complete', 'ordinary-ack', 'threshold-ack'] as const) {
    const small = await measure(10, operation);
    const large = await measure(5000, operation);
    assert.equal(small.calls, 1, `${operation} performs exactly one durable write`);
    assert.deepEqual(large, small, `${operation} persistence and memory are independent of roster size`);
    assert.ok(small.bytes <= 512, `${operation} durable bytes stay within a fixed 512-byte ceiling`);
    evidence[operation] = small;
  }
  assert.deepEqual(
    Object.fromEntries(Object.entries(evidence).map(([operation, value]) => [operation, {
      pendingEvents: value.pendingEvents, eventDigests: value.eventDigests
    }])),
    {
      'ordinary-complete': { pendingEvents: 1, eventDigests: 1 },
      'threshold-complete': { pendingEvents: 1, eventDigests: 1 },
      'ordinary-ack': { pendingEvents: 0, eventDigests: 0 },
      'threshold-ack': { pendingEvents: 0, eventDigests: 0 }
    }
  );
  assert.ok(Math.abs(evidence['threshold-complete'].bytes - evidence['ordinary-complete'].bytes) <= 8,
    'the COMPLETE maintenance boundary adds at most a constant-size delta');
  assert.ok(Math.abs(evidence['threshold-ack'].bytes - evidence['ordinary-ack'].bytes) <= 8,
    'the ACK maintenance boundary adds at most a constant-size delta');
  context.diagnostic(`hot-path persistence evidence ${JSON.stringify(evidence)}`);
});

test('lifecycle v3 maintenance ticks are K-bounded for N=128 and N=5000 and eventually publish', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-maintenance-budget-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const results: Array<{ actorCount: number; maximumCalls: number; maximumBytes: number; ticks: number }> = [];

  for (const actorCount of [128, 5000]) {
    const recordingId = `maintenance-${actorCount}`;
    const outboxRoot = path.join(root, recordingId);
    const sink = new BoundedMeetingIntegrationSink(
      { post: async () => undefined }, logger, 4, 2, 6000,
      { recordingRoot: root, outboxRoot }, lifecycleV3Config
    );
    const journalRoot = path.join(outboxRoot, 'lifecycle-v3', 'pending', `${recordingId}.journal`);
    await mkdir(journalRoot, { recursive: true });
    const actors = Array.from({ length: actorCount }, (_, index) => ({
      actorId: String(10_000_000_000_000_000n + BigInt(index % 1000)), kind: 'human' as const
    }));
    const base = {
      schemaVersion: 2, generation: 0, baseSequence: 128, recordingId,
      guildId: event.guildId, channelId: event.channelId, producer: lifecycleV3Config,
      actorObservationState: 'consistent', actors, sealedReady: null
    };
    (sink as any).publishLifecycleV3Generation(journalRoot, base, 128);
    const state = {
      snapshot: { ...base, emitted: [], pendingOutbox: [] }, actorIndex: new Map(actors.map((actor) => [actor.actorId, actor])),
      pendingEvents: new Map(), eventDigests: new Map(), generation: 0, nextSequence: 129, ackedSequence: 128,
      closed: true, lastOccurredAt: '2026-08-18T00:00:00.000Z', lastAcknowledgedEventId: 'acked',
      lastAcknowledgedDigest: 'a'.repeat(64), maintenanceNeeded: true
    };
    (sink as any).lifecycleV3JournalIndex.set(recordingId, state);
    (sink as any).lifecycleV3MaintenanceQueue.add(recordingId);
    const originalWrite = (sink as any).writeDurableJson.bind(sink);
    let calls = 0;
    let bytes = 0;
    (sink as any).writeDurableJson = (filePath: string, value: unknown) => {
      calls++;
      bytes += Buffer.byteLength(`${JSON.stringify(value)}\n`);
      originalWrite(filePath, value);
    };
    let maximumCalls = 0;
    let maximumBytes = 0;
    let ticks = 0;
    let more: boolean;
    do {
      calls = 0;
      bytes = 0;
      more = sink.runLifecycleV3MaintenanceStep();
      maximumCalls = Math.max(maximumCalls, calls);
      maximumBytes = Math.max(maximumBytes, bytes);
      ticks++;
      if (more) assert.ok(ticks < 2000, 'restartable maintenance must make progress');
    } while (more);
    assert.equal(state.generation, 1);
    assert.equal(state.maintenanceNeeded, false);
    const chunks = (await readdir(journalRoot)).filter((name) => /^generation-00000001-chunk-/.test(name));
    for (const chunk of chunks) {
      const durable = JSON.parse(await readFile(path.join(journalRoot, chunk), 'utf8'));
      assert.ok(durable.records.length >= 1 && durable.records.length <= 8);
    }
    assert.ok(maximumCalls <= 4, `tick used ${maximumCalls} durable calls`);
    results.push({ actorCount, maximumCalls, maximumBytes, ticks });
  }
  assert.ok(results[1].maximumBytes <= results[0].maximumBytes + 16,
    `tick bytes grew with N: ${JSON.stringify(results)}`);
  assert.ok(results[1].ticks > results[0].ticks, 'larger generations complete through more fixed-work ticks');
});

test('production lifecycle maintenance scheduler fairly compacts two interleaved recordings', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-fair-maintenance-scheduler-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const sink = new BoundedMeetingIntegrationSink(
    { post: async () => undefined }, logger, 4, 2, 1024,
    { recordingRoot: root, outboxRoot: root }, lifecycleV3Config
  );
  const recordingIds = ['fair-maintenance-a', 'fair-maintenance-b'];
  for (const [recordingIndex, recordingId] of recordingIds.entries()) {
    const actors = Array.from({ length: 17 + recordingIndex * 2 }, (_, index) => ({
      actorId: String(10_000_000_000_000_000n + BigInt(index)), kind: 'human' as const
    }));
    const journalRoot = path.join(root, 'lifecycle-v3', 'pending', `${recordingId}.journal`);
    await mkdir(journalRoot, { recursive: true });
    const base = {
      schemaVersion: 2, generation: 0, baseSequence: 128, recordingId,
      guildId: event.guildId, channelId: event.channelId, producer: lifecycleV3Config,
      actorObservationState: 'consistent', actors, sealedReady: null
    };
    (sink as any).publishLifecycleV3Generation(journalRoot, base, 128);
    (sink as any).lifecycleV3JournalIndex.set(recordingId, {
      snapshot: { ...base, emitted: [], pendingOutbox: [] },
      actorIndex: new Map(actors.map((actor) => [actor.actorId, actor])), pendingEvents: new Map(), eventDigests: new Map(),
      generation: 0, nextSequence: 129, ackedSequence: 128, closed: true,
      lastOccurredAt: '2026-08-18T00:00:00.000Z', lastAcknowledgedEventId: 'acked',
      lastAcknowledgedDigest: 'a'.repeat(64), maintenanceNeeded: true
    });
    (sink as any).lifecycleV3MaintenanceQueue.add(recordingId);
  }
  const originalStep = (sink as any).runLifecycleV3MaintenanceJournalStep.bind(sink);
  const order: string[] = [];
  let injectedFailures = 2;
  (sink as any).runLifecycleV3MaintenanceJournalStep = (recordingId: string, state: unknown) => {
    order.push(recordingId);
    if (recordingId === recordingIds[0] && injectedFailures-- > 0) throw new Error('injected journal failure');
    return originalStep(recordingId, state);
  };
  (sink as any).scheduleLifecycleV3Maintenance();
  assert.equal(await sink.drain(5000), true, 'scheduled bounded ticks eventually compact both recordings');
  assert.deepEqual(order.slice(0, 6), [
    recordingIds[0], recordingIds[1], recordingIds[0], recordingIds[1], recordingIds[0], recordingIds[1]
  ], 'failed and unfinished journals rotate to the tail after every K-bounded tick');
  for (const recordingId of recordingIds)
    assert.equal((sink as any).lifecycleV3JournalIndex.get(recordingId).generation, 1);
});

test('lifecycle v3 restart and compaction preserve left-only actors and first trusted conflict kind', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-actor-reducer-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingId = 'actor-reducer-recording';
  const producer = createCraigLifecycleV3Producer(lifecycleV3Config, {
    recordingId, guildId: event.guildId, channelId: event.channelId
  });
  const envelope = (eventId: string, occurredAt: string) => ({
    eventId, recordingId, guildId: event.guildId, channelId: event.channelId, occurredAt
  });
  const sink = new BoundedMeetingIntegrationSink(
    { post: async () => undefined }, logger, 8, 2, 1024,
    { recordingRoot: root, outboxRoot: root }, lifecycleV3Config
  );
  const started = producer.started(envelope('actor:start', '2026-08-18T00:00:00.000Z'), [
    { id: event.participantIds[0], bot: false, system: false, webhook: false }
  ]);
  assert.equal(sink.publishLifecycle(started, producer.durableSnapshot()).status, 'accepted');
  const leftOnlyActorId = '1533228054724346099';
  const left = producer.participant(envelope('actor:left', '2026-08-18T00:00:01.000Z'), 'participant.left',
    { id: leftOnlyActorId, bot: false, system: false, webhook: false });
  assert.equal(sink.publishLifecycle(left, producer.durableSnapshot()).status, 'accepted');
  const conflict = producer.participant(envelope('actor:conflict', '2026-08-18T00:00:02.000Z'), 'participant.joined',
    { id: leftOnlyActorId, bot: true, system: false, webhook: false });
  assert.equal(sink.publishLifecycle(conflict, producer.durableSnapshot()).status, 'accepted');
  assert.equal(await sink.drain(2000), true);
  const state = (sink as any).lifecycleV3JournalIndex.get(recordingId);
  state.maintenanceNeeded = true;
  (sink as any).lifecycleV3MaintenanceQueue.add(recordingId);
  for (let ticks = 0; sink.runLifecycleV3MaintenanceStep(); ticks++) assert.ok(ticks < 100);

  const restored = new BoundedMeetingIntegrationSink(
    { post: async () => undefined }, logger, 8, 2, 1024,
    { recordingRoot: root, outboxRoot: root }, lifecycleV3Config
  );
  const snapshot = (restored as any).readLifecycleV3Snapshot(recordingId);
  assert.deepEqual(snapshot.actors, [
    { actorId: event.participantIds[0], kind: 'human' },
    { actorId: leftOnlyActorId, kind: 'human' }
  ]);
  assert.equal(snapshot.actorObservationState, 'conflicted');
});

test('lifecycle v3 recovery rejects torn chunks and descriptors and falls back through manifests', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-generation-corruption-test-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const sink = new BoundedMeetingIntegrationSink(
    { post: async () => undefined }, logger, 4, 2, 1024,
    { recordingRoot: root, outboxRoot: root }, lifecycleV3Config
  );
  const recordingId = 'corruption-fallback';
  const journalRoot = path.join(root, 'lifecycle-v3', 'pending', `${recordingId}.journal`);
  await mkdir(journalRoot, { recursive: true });
  const base = {
    schemaVersion: 2, generation: 0, baseSequence: 128, recordingId,
    guildId: event.guildId, channelId: event.channelId, producer: lifecycleV3Config,
    actorObservationState: 'consistent', actors: [{ actorId: event.participantIds[0], kind: 'human' }], sealedReady: null
  };
  (sink as any).publishLifecycleV3Generation(journalRoot, base, 128);
  const state = {
    snapshot: { ...base, emitted: [], pendingOutbox: [] }, actorIndex: new Map(), pendingEvents: new Map(), eventDigests: new Map(),
    generation: 0, nextSequence: 129, ackedSequence: 128, closed: true,
    lastOccurredAt: '2026-08-18T00:00:00.000Z', lastAcknowledgedEventId: 'acked',
    lastAcknowledgedDigest: 'a'.repeat(64), maintenanceNeeded: true
  };
  (sink as any).lifecycleV3JournalIndex.set(recordingId, state);
  (sink as any).lifecycleV3MaintenanceQueue.add(recordingId);
  for (let ticks = 0; sink.runLifecycleV3MaintenanceStep(); ticks++) assert.ok(ticks < 100);
  const manifestPath = path.join(journalRoot, 'manifest.json');
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
  const descriptorPath = path.join(journalRoot, manifest.current.file);
  const descriptor = await readFile(descriptorPath, 'utf8');
  const chunkPath = path.join(journalRoot, 'generation-00000001-chunk-00000000.json');
  const chunk = await readFile(chunkPath, 'utf8');

  await writeFile(chunkPath, '{"torn":');
  assert.equal((sink as any).recoverLifecycleV3Generation(journalRoot).generation, 0);
  await writeFile(chunkPath, chunk);
  await writeFile(descriptorPath, '{"torn":');
  assert.equal((sink as any).recoverLifecycleV3Generation(journalRoot).generation, 0);
  await writeFile(descriptorPath, descriptor);
  await writeFile(manifestPath, '{"torn":');
  assert.equal((sink as any).recoverLifecycleV3Generation(journalRoot).generation, 0,
    'a torn current manifest selects only the separately referenced previous generation');

  await writeFile(manifestPath, `${JSON.stringify(manifest)}\n`);
  const corruptChunk = JSON.parse(chunk);
  corruptChunk.checksumSha256 = '0'.repeat(64);
  await writeFile(chunkPath, `${JSON.stringify(corruptChunk)}\n`);
  assert.equal((sink as any).recoverLifecycleV3Generation(journalRoot).generation, 0,
    'valid JSON with a corrupt chunk checksum falls back to previous');
  await writeFile(chunkPath, chunk);
  const corruptDescriptor = JSON.parse(descriptor);
  corruptDescriptor.checksumSha256 = '0'.repeat(64);
  await writeFile(descriptorPath, `${JSON.stringify(corruptDescriptor)}\n`);
  assert.equal((sink as any).recoverLifecycleV3Generation(journalRoot).generation, 0,
    'generation descriptor/final checksum corruption falls back to previous');

  await writeFile(manifestPath, `${JSON.stringify(manifest)}\n`);
  await writeFile(descriptorPath, '{"torn":');
  await writeFile(path.join(journalRoot, manifest.previous.file), '{"torn":');
  await writeFile(path.join(journalRoot, 'manifest.previous.json'), '{"torn":');
  assert.equal((sink as any).recoverLifecycleV3Generation(journalRoot), undefined,
    'current and previous invalid fails closed without selecting an orphan generation');
});

test('lifecycle v3 table-driven crash matrix recovers every durable maintenance phase without selecting partial generations', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-crash-matrix-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const actors = Array.from({ length: 19 }, (_, index) => ({
    actorId: String(10_000_000_000_000_000n + BigInt(index)), kind: 'human' as const
  }));
  const points = [
    { name: 'capture-target', match: (file: string) => file.endsWith('maintenance.json'), occurrence: 1 },
    { name: 'every-chunk-write-first', match: (file: string) => file.includes('-chunk-00000000.json'), occurrence: 1 },
    { name: 'every-chunk-write-middle', match: (file: string) => file.includes('-chunk-00000001.json'), occurrence: 1 },
    { name: 'every-chunk-write-last', match: (file: string) => file.includes('-chunk-00000002.json'), occurrence: 1 },
    { name: 'generation-descriptor-final-checksum', match: (file: string) => file.endsWith('snapshot-00000001.json'), occurrence: 1 },
    { name: 'previous-manifest-publication', match: (file: string) => file.endsWith('manifest.previous.json'), occurrence: 1 },
    { name: 'current-manifest-publication', match: (file: string) => file.endsWith('manifest.json'), occurrence: 1 },
    { name: 'delta-cleanup-cursor-and-steps', match: (file: string, value: any) => file.endsWith('maintenance.json') && value.phase === 'cleanup-deltas', occurrence: 1 },
    { name: 'generation-cleanup-cursor-and-steps', match: (file: string, value: any) => file.endsWith('maintenance.json') && value.phase === 'cleanup-generations', occurrence: 1 }
  ];

  for (const point of points) for (const side of ['before', 'after'] as const) {
    const recordingId = `${point.name}-${side}`.replace(/[^a-z0-9-]/g, '-');
    const outboxRoot = path.join(root, recordingId);
    const makeSink = () => new BoundedMeetingIntegrationSink(
      { post: async () => undefined }, logger, 4, 2, 6000,
      { recordingRoot: root, outboxRoot }, lifecycleV3Config
    );
    let sink = makeSink();
    const journalRoot = path.join(outboxRoot, 'lifecycle-v3', 'pending', `${recordingId}.journal`);
    await mkdir(journalRoot, { recursive: true });
    const base = {
      schemaVersion: 2, generation: 0, baseSequence: 0, recordingId,
      guildId: event.guildId, channelId: event.channelId, producer: lifecycleV3Config,
      actorObservationState: 'consistent', actors, sealedReady: null
    };
    (sink as any).publishLifecycleV3Generation(journalRoot, base, 0);
    const started = {
      schemaVersion: 3, eventId: `${recordingId}:start`, recordingId, guildId: event.guildId, channelId: event.channelId,
      occurredAt: '2026-08-18T00:00:00.000Z', type: 'meeting.started', actorSemanticsVersion,
      producerCapabilityId: sealedActorRosterCapabilityId, producerRevision: lifecycleV3Config.producerRevision,
      actorObservationState: 'consistent', actors, rosterState: 'unsealed'
    };
    for (let sequence = 0; sequence <= 5; sequence++) await writeFile(
      path.join(journalRoot, `${String(sequence).padStart(8, '0')}.event.json`),
      `${JSON.stringify(sequence === 0 ? started : {
        ...started, eventId: `${recordingId}:${sequence}`, occurredAt: `2026-08-18T00:00:0${sequence}.000Z`,
        type: 'participant.joined', actor: actors[sequence], actors: undefined, rosterState: undefined
      })}\n`
    );
    const digest = createHash('sha256').update(JSON.stringify(started)).digest('hex');
    await writeFile(path.join(journalRoot, 'cursor.json'), `${JSON.stringify({
      schemaVersion: 1, generation: 0, ackedSequence: 4, eventId: `${recordingId}:4`, digestSha256: 'a'.repeat(64)
    })}\n`);
    const state = {
      snapshot: { ...base, emitted: [], pendingOutbox: [] }, actorIndex: new Map(actors.map((actor) => [actor.actorId, actor])),
      pendingEvents: new Map(), eventDigests: new Map(), generation: 0, nextSequence: 6, ackedSequence: 4,
      closed: false, lastOccurredAt: '2026-08-18T00:00:05.000Z', lastAcknowledgedEventId: `${recordingId}:4`,
      lastAcknowledgedDigest: digest, maintenanceNeeded: true
    };
    (sink as any).lifecycleV3JournalIndex.set(recordingId, state);
    (sink as any).lifecycleV3MaintenanceQueue.add(recordingId);
    const durableWrite = (sink as any).writeDurableJson.bind(sink);
    let seen = 0;
    let crashed = false;
    (sink as any).writeDurableJson = (file: string, value: unknown) => {
      if (point.match(file, value) && ++seen === point.occurrence) {
        if (side === 'before') throw new Error(`injected ${point.name} before`);
        durableWrite(file, value);
        crashed = true;
        throw new Error(`injected ${point.name} after`);
      }
      durableWrite(file, value);
    };
    try { for (let ticks = 0; sink.runLifecycleV3MaintenanceStep(); ticks++) assert.ok(ticks < 100); }
    catch (error) { assert.match(String(error), /injected/); crashed = true; }
    assert.equal(crashed, true, `${point.name}/${side} was exercised`);

    sink = makeSink();
    const recovered = (sink as any).recoverLifecycleV3Generation(journalRoot);
    assert.ok(recovered?.generation === 0 || recovered?.generation === 1, 'only a complete manifest-referenced generation is selected');
    const recoveredActors = new Set(recovered.actors.map((actor: any) => actor.actorId));
    assert.equal(recoveredActors.size, recovered.actors.length, 'no actor/event effect is applied twice');
    assert.ok(await readFile(path.join(journalRoot, '00000005.event.json'), 'utf8'), 'post-capture delta is preserved');
    const cursor = JSON.parse(await readFile(path.join(journalRoot, 'cursor.json'), 'utf8'));
    assert.equal(cursor.ackedSequence, 4, 'acknowledged cursor survives every crash point');
  }
});

test('lifecycle v3 long-run append ACK compaction and restart keeps physical indexes bounded and duplicate ACK idempotent', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'craig-v3-long-run-'));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const recordingId = 'long-run-append-ack';
  const makeSink = () => new BoundedMeetingIntegrationSink(
    { post: async () => undefined }, logger, 4, 2, 6000,
    { recordingRoot: root, outboxRoot: root }, lifecycleV3Config
  );
  let sink = makeSink();
  (sink as any).scheduleLifecycleV3Maintenance = () => undefined;
  const firstActor = { actorId: '10000000000000000', kind: 'human' as const };
  const admission = {
    schemaVersion: 2 as const, recordingId, guildId: event.guildId, channelId: event.channelId,
    producer: {
      actorSemanticsVersion, producerCapabilityId: sealedActorRosterCapabilityId,
      producerRevision: lifecycleV3Config.producerRevision
    },
    actorObservationState: 'consistent' as const, actors: [firstActor], sealedReady: null
  };
  const started = {
    schemaVersion: 3 as const, eventId: `${recordingId}:0`, recordingId, guildId: event.guildId, channelId: event.channelId,
    occurredAt: '2026-08-18T00:00:00.000Z', type: 'meeting.started' as const, actorSemanticsVersion,
    producerCapabilityId: sealedActorRosterCapabilityId, producerRevision: lifecycleV3Config.producerRevision,
    actorObservationState: 'consistent' as const, actors: [firstActor], rosterState: 'unsealed' as const
  };
  (sink as any).appendLifecycleV3Event(admission, started, 0);
  (sink as any).acknowledgeLifecycleV3Event(started);
  let last: any = started;
  for (let sequence = 1; sequence <= 1024; sequence++) {
    last = {
      schemaVersion: 3 as const, eventId: `${recordingId}:${sequence}`, recordingId,
      guildId: event.guildId, channelId: event.channelId,
      occurredAt: new Date(Date.parse('2026-08-18T00:00:00.000Z') + sequence).toISOString(),
      type: 'participant.joined' as const, actorSemanticsVersion,
      producerCapabilityId: sealedActorRosterCapabilityId, producerRevision: lifecycleV3Config.producerRevision,
      actorObservationState: 'consistent' as const,
      actor: { actorId: String(10_000_000_000_000_000n + BigInt(sequence % 1000)), kind: 'human' as const }
    };
    (sink as any).appendLifecycleV3Event(undefined, last, sequence);
    (sink as any).acknowledgeLifecycleV3Event(last);
    const indexed = (sink as any).lifecycleV3JournalIndex.get(recordingId);
    assert.equal(indexed.pendingEvents.size, 0, `pendingEvents bounded after cycle ${sequence}`);
    assert.equal(indexed.eventDigests.size, 0, `eventDigests bounded after cycle ${sequence}`);
    if (sequence % 4 === 0) sink.runLifecycleV3MaintenanceStep();
  }
  for (let ticks = 0; sink.runLifecycleV3MaintenanceStep(); ticks++) assert.ok(ticks < 5000, 'compaction eventually completes');

  sink = makeSink();
  const recovered = (sink as any).readLifecycleV3Journal(recordingId);
  assert.equal(recovered.ackedSequence, 1024);
  assert.equal(recovered.pendingEvents.size, 0);
  assert.equal(recovered.eventDigests.size, 0);
  assert.equal(recovered.actorIndex.size, 1000, 'post-capture deltas survive interleaved compaction and restart within roster cap');
  (sink as any).acknowledgeLifecycleV3Event(last);
  assert.equal(recovered.ackedSequence, 1024, 'duplicate ACK after restart is idempotent');
});
