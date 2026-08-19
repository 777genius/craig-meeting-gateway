import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';

import {
  actorSemanticsVersion,
  createCraigLifecycleV3Producer,
  deriveCraigActorFromDiscord,
  maximumCraigActorRosterSize,
  maximumCraigPendingLifecycleEvents,
  maximumE2eSyntheticHumanActors,
  parseMeetingLifecycleProducerConfiguration,
  restoreCraigLifecycleV3ProducerFromSnapshot,
  sealedActorRosterCapabilityId,
  selectCraigKnowledgeEligibleActorIds
} from './meetingLifecycleV3';

const producerRevision = '0123456789abcdef0123456789abcdef01234567';
const producer = { actorSemanticsVersion, producerCapabilityId: sealedActorRosterCapabilityId, producerRevision } as const;
const config = { schemaVersion: 3 as const, ...producer };
const context = {
  recordingId: 'recording-1',
  guildId: '1533228590643155034',
  channelId: '1533228823045214398'
};
const envelope = { ...context, eventId: 'recording-1:1', occurredAt: '2026-08-13T00:00:00.000Z' };
const human = { id: '1533227577286852649', bot: false, system: false, webhook: false } as const;
const automation = { id: '1533228054724346087', bot: true, system: false, webhook: false } as const;

test('keeps schema v1 as the strict default and requires an explicit v3 capability', () => {
  assert.deepEqual(parseMeetingLifecycleProducerConfiguration({ schemaVersion: 1 }), { schemaVersion: 1 });
  assert.throws(
    () => parseMeetingLifecycleProducerConfiguration({ schemaVersion: 1, producerCapabilityId: sealedActorRosterCapabilityId }),
    /cannot claim v3 capabilities/
  );
  assert.deepEqual(parseMeetingLifecycleProducerConfiguration(config), config);
});

test('derives actor kind only from authenticated Discord signals and fails closed when incomplete', () => {
  assert.deepEqual(deriveCraigActorFromDiscord(human), { actorId: human.id, kind: 'human' });
  for (const signal of [{ bot: true }, { system: true }, { webhook: true }])
    assert.equal(deriveCraigActorFromDiscord({ id: automation.id, ...signal }).kind, 'automation');
  assert.equal(deriveCraigActorFromDiscord({ id: human.id }).kind, 'unknown');
  assert.throws(() => deriveCraigActorFromDiscord({ id: human.id, kind: 'human' } as never), /invalid/);
});

test('maps only explicitly allowlisted E2E bots to synthetic humans behind the test-only guard', () => {
  const e2eConfig = parseMeetingLifecycleProducerConfiguration({
    ...config,
    e2eTestOnly: true,
    e2eSyntheticHumanActorIds: [automation.id]
  });
  assert.equal(e2eConfig.schemaVersion, 3);
  if (e2eConfig.schemaVersion !== 3) throw new Error('Expected lifecycle v3');
  const lifecycle = createCraigLifecycleV3Producer(e2eConfig, context);
  const started = lifecycle.started(envelope, [automation, human]);
  assert.deepEqual(started.actors, [
    { actorId: human.id, kind: 'human' },
    { actorId: automation.id, kind: 'human' }
  ]);
  assert.equal(deriveCraigActorFromDiscord({ ...automation, system: true }, new Set([automation.id])).kind, 'automation');
  assert.equal(deriveCraigActorFromDiscord({ ...automation, webhook: true }, new Set([automation.id])).kind, 'automation');
  assert.equal(deriveCraigActorFromDiscord({ ...automation, bot: false }, new Set([automation.id])).kind, 'human');
  const joined = lifecycle.participant(
    { ...envelope, eventId: 'recording-1:2', occurredAt: '2026-08-13T00:00:01.000Z' },
    'participant.joined',
    automation
  );
  assert.deepEqual(joined.actor, { actorId: automation.id, kind: 'human' });
  const restored = createCraigLifecycleV3Producer(e2eConfig, context, lifecycle.durableSnapshot());
  const left = restored.participant(
    { ...envelope, eventId: 'recording-1:3', occurredAt: '2026-08-13T00:00:02.000Z' },
    'participant.left',
    automation
  );
  assert.deepEqual(left.actor, { actorId: automation.id, kind: 'human' });
  assert.throws(() => createCraigLifecycleV3Producer(config, context, lifecycle.durableSnapshot()), /another actor classification policy/);
  assert.deepEqual(restoreCraigLifecycleV3ProducerFromSnapshot(lifecycle.durableSnapshot()).durableSnapshot(), lifecycle.durableSnapshot());
});

test('rejects malformed or unguarded E2E synthetic human actor configuration', () => {
  assert.equal(maximumE2eSyntheticHumanActors, 128);
  assert.throws(() => parseMeetingLifecycleProducerConfiguration({ ...config, e2eSyntheticHumanActorIds: [automation.id] }), /unsupported version/);
  assert.throws(
    () =>
      parseMeetingLifecycleProducerConfiguration({
        ...config,
        e2eTestOnly: false,
        e2eSyntheticHumanActorIds: [automation.id]
      }),
    /test-only guard/
  );
  for (const actorIds of [
    [],
    ['invalid'],
    [automation.id, automation.id],
    Array.from({ length: maximumE2eSyntheticHumanActors + 1 }, (_, index) => String(10000000000000000n + BigInt(index)))
  ])
    assert.throws(
      () =>
        parseMeetingLifecycleProducerConfiguration({
          ...config,
          e2eTestOnly: true,
          e2eSyntheticHumanActorIds: actorIds
        }),
      /identities are invalid/
    );
});

test('generates real v3 envelopes, copies producer input, and seals a sorted roster', () => {
  const mutable: any = { ...config };
  const lifecycle = createCraigLifecycleV3Producer(mutable, context);
  mutable.producerRevision = 'f'.repeat(40);
  const started = lifecycle.started(envelope, [automation, human]);
  assert.equal(started.producerRevision, producerRevision);
  assert.equal(Object.isFrozen(started), true);
  assert.equal(Object.isFrozen(started.actors), true);
  assert.throws(() => Object.assign(started, { recordingId: 'mutated' }), TypeError);
  assert.deepEqual(started.actors, [
    { actorId: human.id, kind: 'human' },
    { actorId: automation.id, kind: 'automation' }
  ]);
  const readyEnvelope = { ...envelope, eventId: 'recording-1:ready', occurredAt: '2026-08-13T00:01:00.000Z' };
  const input = {
    actors: [human, automation],
    endedAt: readyEnvelope.occurredAt,
    sourceFilesChecksumSha256: 'a'.repeat(64),
    trackCount: 2
  };
  const ready = lifecycle.authoritativeReady(readyEnvelope, input);
  assert.equal(ready.type, 'recording.authoritative_ready');
  assert.deepEqual(lifecycle.authoritativeReady(readyEnvelope, input), ready);
  const restored = restoreCraigLifecycleV3ProducerFromSnapshot(JSON.parse(JSON.stringify(lifecycle.durableSnapshot())));
  assert.deepEqual(restored.authoritativeReady(readyEnvelope, input), ready);
  assert.throws(() => lifecycle.participant({ ...readyEnvelope, eventId: 'later' }, 'participant.joined', human), /sealed/);
  assert.throws(() => lifecycle.connection({ ...readyEnvelope, eventId: 'later' }, 'meeting.connection_lost', null), /sealed/);
  assert.throws(() => lifecycle.terminal({ ...readyEnvelope, eventId: 'later' }, 'meeting.ended', null), /sealed/);
  assert.throws(() => lifecycle.observeActors([human]), /sealed/);
  assert.throws(() => lifecycle.authoritativeReady(readyEnvelope, { ...input, trackCount: 3 }), /Conflicting authoritative-ready retry/);
});

test('validates actor batches transactionally before any ledger mutation', () => {
  const lifecycle = createCraigLifecycleV3Producer(config, context);
  assert.equal(maximumCraigActorRosterSize, 1_000);
  const before = lifecycle.durableSnapshot();
  const oversized = Array.from({ length: maximumCraigActorRosterSize + 1 }, (_, index) => ({
    id: String(10000000000000000n + BigInt(index)),
    bot: false,
    system: false,
    webhook: false
  }));
  assert.throws(() => lifecycle.started(envelope, oversized), /bounded size/);
  assert.deepEqual(lifecycle.durableSnapshot(), before);
  assert.throws(() => lifecycle.started(envelope, [human, { ...automation, id: 'invalid' }]), /invalid/);
  assert.deepEqual(lifecycle.durableSnapshot(), before);
});

test('marks contradictory authenticated identity evidence conflicted and filters it closed', () => {
  const lifecycle = createCraigLifecycleV3Producer(config, context);
  lifecycle.started(envelope, [human]);
  lifecycle.participant({ ...envelope, eventId: 'recording-1:2', occurredAt: '2026-08-13T00:00:01.000Z' }, 'participant.joined', {
    ...human,
    bot: true
  });
  const snapshot = lifecycle.durableSnapshot();
  assert.equal(snapshot.actorObservationState, 'conflicted');
  assert.deepEqual(snapshot.actors, [{ actorId: human.id, kind: 'human' }]);
  const ready = lifecycle.authoritativeReady(
    { ...envelope, eventId: 'recording-1:ready', occurredAt: '2026-08-13T00:00:02.000Z' },
    { actors: [human], endedAt: '2026-08-13T00:00:02.000Z', sourceFilesChecksumSha256: 'a'.repeat(64), trackCount: 1 }
  );
  assert.deepEqual(selectCraigKnowledgeEligibleActorIds(ready, [human.id]), []);
});

test('filters automation, unknown, and untracked identities from a consistent sealed roster', () => {
  const unknown = { id: '1533228590643155035' } as const;
  const lifecycle = createCraigLifecycleV3Producer(config, context);
  lifecycle.started(envelope, [human, automation, unknown]);
  const ready = lifecycle.authoritativeReady(
    { ...envelope, eventId: 'recording-1:ready', occurredAt: '2026-08-13T00:00:02.000Z' },
    {
      actors: [human, automation, unknown],
      endedAt: '2026-08-13T00:00:02.000Z',
      sourceFilesChecksumSha256: 'a'.repeat(64),
      trackCount: 3
    }
  );
  assert.deepEqual(selectCraigKnowledgeEligibleActorIds(ready, [automation.id, unknown.id, human.id]), [human.id]);
});

test('snapshot binds producer, recording context, event order, and pending exact replay', () => {
  const lifecycle = createCraigLifecycleV3Producer(config, context);
  lifecycle.started(envelope, [human]);
  lifecycle.participant({ ...envelope, eventId: 'recording-1:2', occurredAt: '2026-08-13T00:00:01.000Z' }, 'participant.joined', automation);
  const snapshot = JSON.parse(JSON.stringify(lifecycle.durableSnapshot()));
  assert.deepEqual(restoreCraigLifecycleV3ProducerFromSnapshot(snapshot).durableSnapshot(), snapshot);
  const missingClassificationPolicy = JSON.parse(JSON.stringify(snapshot));
  delete missingClassificationPolicy.actorClassificationPolicy;
  assert.throws(
    () => restoreCraigLifecycleV3ProducerFromSnapshot(missingClassificationPolicy),
    /classification policy is malformed/
  );
  assert.deepEqual(
    snapshot.emitted,
    snapshot.pendingOutbox.map(({ eventId, occurredAt, type }: any) => ({ eventId, occurredAt, type }))
  );
  assert.throws(
    () => createCraigLifecycleV3Producer(config, { ...context, channelId: '1533228823045214399' }, snapshot),
    /another recording context/
  );
  assert.throws(
    () => createCraigLifecycleV3Producer({ ...config, producerRevision: 'f'.repeat(40) }, context, snapshot),
    /another producer revision/
  );
  const unbound = JSON.parse(JSON.stringify(lifecycle.durableSnapshot()));
  unbound.emitted.pop();
  assert.throws(() => restoreCraigLifecycleV3ProducerFromSnapshot(unbound), /not bound/);
  const missingPinned = JSON.parse(JSON.stringify(lifecycle.durableSnapshot()));
  missingPinned.pendingOutbox.shift();
  assert.throws(() => restoreCraigLifecycleV3ProducerFromSnapshot(missingPinned), /omits pinned evidence/);
  snapshot.pendingOutbox[0].recordingId = 'other-recording';
  assert.throws(() => restoreCraigLifecycleV3ProducerFromSnapshot(snapshot), /another recording context/);
});

test('rejects producer and envelope unknown keys exactly', () => {
  assert.throws(() => parseMeetingLifecycleProducerConfiguration({ ...config, extra: true }), /unsupported version/);
  const lifecycle = createCraigLifecycleV3Producer(config, context);
  assert.throws(() => lifecycle.started({ ...envelope, extra: true } as never, [human]), /unknown fields/);
  assert.deepEqual(lifecycle.durableSnapshot().actors, []);
});

test('rejects expanded-year lifecycle timestamps before durable evidence is admitted', () => {
  const expandedYear = '+010000-01-01T00:00:00.000Z';
  const lifecycle = createCraigLifecycleV3Producer(config, context);
  assert.throws(() => lifecycle.started({ ...envelope, occurredAt: expandedYear }, [human]), /event identity/);
  assert.deepEqual(lifecycle.durableSnapshot().pendingOutbox, []);

  lifecycle.started(envelope, [human]);
  assert.throws(
    () =>
      lifecycle.authoritativeReady(
        { ...envelope, eventId: 'recording-1:ready', occurredAt: '2026-08-13T00:01:00.000Z' },
        { actors: [human], endedAt: expandedYear, sourceFilesChecksumSha256: 'a'.repeat(64), trackCount: 1 }
      ),
    /authoritative-ready evidence/
  );
  assert.equal(lifecycle.durableSnapshot().pendingOutbox.length, 1);
});

test('reserves terminal and authoritative-ready capacity at the durable journal bound', () => {
  const lifecycle = createCraigLifecycleV3Producer(config, context);
  lifecycle.started(envelope, [human]);
  for (let index = 1; index < maximumCraigPendingLifecycleEvents - 2; index++)
    lifecycle.connection(
      { ...envelope, eventId: `recording-1:${index + 1}` },
      index % 2 === 0 ? 'meeting.connection_recovered' : 'meeting.connection_lost',
      null
    );
  const before = lifecycle.durableSnapshot();
  assert.equal(before.pendingOutbox.length, maximumCraigPendingLifecycleEvents - 2);
  assert.throws(
    () => lifecycle.connection({ ...envelope, eventId: 'recording-1:overflow' }, 'meeting.connection_lost', null),
    /capacity is exhausted/
  );
  assert.deepEqual(lifecycle.durableSnapshot(), before);
  lifecycle.terminal({ ...envelope, eventId: 'recording-1:terminal' }, 'meeting.ended', null);
  lifecycle.authoritativeReady(
    { ...envelope, eventId: 'recording-1:ready' },
    { actors: [human], endedAt: envelope.occurredAt, sourceFilesChecksumSha256: 'a'.repeat(64), trackCount: 1 }
  );
  assert.equal(lifecycle.durableSnapshot().pendingOutbox.length, maximumCraigPendingLifecycleEvents);
});

test('evicts acknowledged payloads and survives restart beyond 5,000 production transitions', () => {
  let lifecycle = createCraigLifecycleV3Producer(config, context);
  const started = lifecycle.started(envelope, [human]);
  lifecycle.acknowledgeDelivered(started.eventId);

  for (let index = 1; index <= 5_000; index++) {
    const event = lifecycle.connection(
      { ...envelope, eventId: `recording-1:connection:${index}` },
      index % 2 === 0 ? 'meeting.connection_recovered' : 'meeting.connection_lost',
      null
    );
    lifecycle.acknowledgeDelivered(event.eventId);
    if (index % 1_000 === 0) lifecycle = restoreCraigLifecycleV3ProducerFromSnapshot(lifecycle.durableSnapshot());
  }

  const terminal = lifecycle.terminal({ ...envelope, eventId: 'recording-1:terminal' }, 'meeting.ended', null);
  lifecycle.acknowledgeDelivered(terminal.eventId);
  lifecycle = restoreCraigLifecycleV3ProducerFromSnapshot(lifecycle.durableSnapshot());
  lifecycle.authoritativeReady(
    { ...envelope, eventId: 'recording-1:ready' },
    { actors: [human], endedAt: envelope.occurredAt, sourceFilesChecksumSha256: 'a'.repeat(64), trackCount: 1 }
  );
  assert.deepEqual(
    lifecycle.durableSnapshot().pendingOutbox.map(({ type }) => type),
    ['meeting.started', 'meeting.ended', 'recording.authoritative_ready']
  );
  assert.equal(lifecycle.durableSnapshot().emitted.length, 5_003);
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
});
