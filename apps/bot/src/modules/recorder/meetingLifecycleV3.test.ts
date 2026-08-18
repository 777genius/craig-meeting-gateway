import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';

import {
  actorSemanticsVersion,
  CraigActorObservationLedger,
  createCraigLifecycleV3Producer,
  parseMeetingLifecycleProducerConfiguration,
  sealedActorRosterCapabilityId
} from './meetingLifecycleV3';

const producerRevision = '0123456789abcdef0123456789abcdef01234567';
const producer = {
  actorSemanticsVersion,
  producerCapabilityId: sealedActorRosterCapabilityId,
  producerRevision
} as const;
const config = { schemaVersion: 3, ...producer } as const;
const envelope = {
  eventId: 'recording-1:1',
  recordingId: 'recording-1',
  guildId: '1533228590643155034',
  channelId: '1533228823045214398',
  occurredAt: '2026-08-13T00:00:00.000Z'
} as const;
const human = { actorId: '1533227577286852649', kind: 'human' } as const;
const automation = { actorId: '1533228054724346087', kind: 'automation' } as const;

test('keeps legacy v1 and trusted v3 producer selection explicit during rolling upgrade', () => {
  assert.deepEqual(parseMeetingLifecycleProducerConfiguration({ schemaVersion: 1 }), { schemaVersion: 1 });
  assert.deepEqual(parseMeetingLifecycleProducerConfiguration(config), config);
  assert.throws(() => parseMeetingLifecycleProducerConfiguration({ schemaVersion: 1, producerRevision }), /cannot claim v3 capabilities/);
  assert.throws(() => parseMeetingLifecycleProducerConfiguration({ ...config, schemaVersion: 2 }), /unsupported version/);
});

test('generates exact v3 envelopes and seals the complete sorted actor roster', () => {
  const lifecycle = createCraigLifecycleV3Producer(config);
  assert.deepEqual(lifecycle.started(envelope, [automation, human]), {
    ...envelope,
    schemaVersion: 3,
    ...producer,
    actorObservationState: 'consistent',
    type: 'meeting.started',
    actors: [human, automation],
    rosterState: 'unsealed'
  });
  const lateHuman = { actorId: '1533228590643155035', kind: 'human' } as const;
  lifecycle.participant({ ...envelope, eventId: 'recording-1:2' }, 'participant.joined', lateHuman);
  assert.deepEqual(
    lifecycle.authoritativeReady(
      { ...envelope, eventId: 'recording-1:authoritative-ready:v3', occurredAt: '2026-08-13T00:01:00.000Z' },
      {
        actors: [automation, lateHuman, human],
        endedAt: '2026-08-13T00:01:00.000Z',
        sourceFilesChecksumSha256: 'a'.repeat(64),
        trackCount: 3
      }
    ),
    {
      ...envelope,
      eventId: 'recording-1:authoritative-ready:v3',
      occurredAt: '2026-08-13T00:01:00.000Z',
      schemaVersion: 3,
      ...producer,
      actorObservationState: 'consistent',
      type: 'recording.authoritative_ready',
      actors: [human, automation, lateHuman],
      rosterState: 'sealed',
      endedAt: '2026-08-13T00:01:00.000Z',
      sourceFilesChecksumSha256: 'a'.repeat(64),
      trackCount: 3
    }
  );
});

test('persists actor evidence and rejects old/new producer overlap after restart', () => {
  const lifecycle = createCraigLifecycleV3Producer(config);
  lifecycle.started(envelope, [human]);
  const snapshot = lifecycle.durableSnapshot();
  const restored = createCraigLifecycleV3Producer(config, JSON.parse(JSON.stringify(snapshot)));
  assert.deepEqual(restored.durableSnapshot(), snapshot);
  assert.throws(() => CraigActorObservationLedger.restore(snapshot, { ...producer, producerRevision: 'f'.repeat(40) }), /another producer revision/);
});

test('keeps contradictory actor observations permanently conflicted through sealing', () => {
  const lifecycle = createCraigLifecycleV3Producer(config);
  lifecycle.started(envelope, [human]);
  lifecycle.participant({ ...envelope, eventId: 'recording-1:2' }, 'participant.joined', {
    actorId: human.actorId,
    kind: 'automation'
  });
  assert.equal(lifecycle.durableSnapshot().actorObservationState, 'conflicted');
  const ready = lifecycle.authoritativeReady(
    { ...envelope, eventId: 'recording-1:ready' },
    { actors: [human], endedAt: envelope.occurredAt, sourceFilesChecksumSha256: 'a'.repeat(64), trackCount: 1 }
  );
  assert.equal(ready.actorObservationState, 'conflicted');
});

test('producer-owned canonical bundle has the exact bytes pinned by the Meeting consumer', async () => {
  const root = path.resolve(__dirname, '../../../contracts/craig-lifecycle-v3');
  const [schema, fixtures, sums, bundle] = await Promise.all([
    readFile(path.join(root, 'craig-lifecycle-v3.schema.json')),
    readFile(path.join(root, 'canonical-fixtures.json')),
    readFile(path.join(root, 'SHA256SUMS')),
    readFile(path.join(root, 'BUNDLE.sha256'))
  ]);
  const digest = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex');
  assert.equal(digest(schema), 'aab972f39dde6e5336b8301c2da8204ad5c99e881a9432bec87db41020230f8d');
  assert.equal(digest(fixtures), 'b8d5b86ee248dcf2823fdfcc7a610d0197a4bdfbdd0963c17ddb7b4d2c4d3f9e');
  assert.equal(digest(sums), '43b58c2661b22039fa432199227318b0d91fbbe1faa669bc0e62a68ddff8f940');
  assert.equal(digest(bundle), '9ecdba8ebe3dd7e5ca4d67be0d540a66d07c3a66e0536dcd9c929099249f72a9');
  assert.equal(bundle.toString('utf8'), `${digest(sums)}  SHA256SUMS\n`);
  const parsed = JSON.parse(fixtures.toString('utf8')) as { contract: string; producerRevision: string };
  assert.equal(parsed.contract, 'craig-lifecycle-v3');
  assert.equal(parsed.producerRevision, producerRevision);
});
