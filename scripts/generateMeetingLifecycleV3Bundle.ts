import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import {
  type AuthenticatedDiscordActor,
  type CraigActor,
  type CraigLifecycleEnvelope,
  type CraigLifecycleV3Event,
  actorSemanticsVersion,
  createCraigLifecycleV3Producer,
  maximumCraigActorRosterSize,
  meetingLifecycleV3SchemaVersion,
  sealedActorRosterCapabilityId,
  selectCraigKnowledgeEligibleActorIds,
  validateCraigLifecycleV3Event
} from '../apps/bot/src/modules/recorder/meetingLifecycleV3';

const root = path.resolve(__dirname, '../apps/bot/contracts/craig-lifecycle-v3');
const schemaFileName = 'craig-lifecycle-v3.schema.json';
const fixturesFileName = 'canonical-fixtures.json';
const checksumFileName = 'SHA256SUMS';
const bundleFileName = 'BUNDLE.sha256';

const digest = (bytes: Uint8Array): string => createHash('sha256').update(bytes).digest('hex');

interface CanonicalFixture {
  name: string;
  expectedKnowledgeEligibleActorIds: string[];
  authoritativeTrackActorIds: string[];
  actorObservations?: CraigActor[];
  events: CraigLifecycleV3Event[];
}

interface CanonicalFixtures {
  bundleVersion: number;
  contract: string;
  producerCapabilityId: string;
  actorSemanticsVersion: number;
  producerRevision: string;
  fixtures: CanonicalFixture[];
}

interface ContractSchemaConstants {
  $defs: {
    actors: { maxItems: number };
    envelope: {
      properties: {
        schemaVersion: { const: number };
        producerCapabilityId: { const: string };
        actorSemanticsVersion: { const: number };
      };
    };
  };
}

function authenticatedActor({ actorId, kind }: CraigActor): AuthenticatedDiscordActor {
  if (kind === 'automation') return { id: actorId, bot: true, system: false, webhook: false };
  if (kind === 'human') return { id: actorId, bot: false, system: false, webhook: false };
  return { id: actorId };
}

function eventEnvelope(event: CraigLifecycleV3Event): CraigLifecycleEnvelope {
  const { eventId, recordingId, guildId, channelId, occurredAt } = event;
  return { eventId, recordingId, guildId, channelId, occurredAt };
}

function assertFixtureBuiltByRuntime(fixture: CanonicalFixture, producerRevision: string): void {
  assert.ok(fixture.events.length > 0, `${fixture.name} has no lifecycle events`);
  const first = fixture.events[0];
  const lifecycle = createCraigLifecycleV3Producer(
    { schemaVersion: 3, actorSemanticsVersion, producerCapabilityId: sealedActorRosterCapabilityId, producerRevision },
    { recordingId: first.recordingId, guildId: first.guildId, channelId: first.channelId }
  );
  let appliedObservations = false;
  for (const expected of fixture.events) {
    let actual: CraigLifecycleV3Event;
    if (expected.type === 'meeting.started') {
      actual = lifecycle.started(eventEnvelope(expected), expected.actors.map(authenticatedActor));
    } else {
      if (!appliedObservations && fixture.actorObservations !== undefined) {
        lifecycle.observeActors(fixture.actorObservations.map(authenticatedActor));
        appliedObservations = true;
      }
      if (expected.type === 'participant.joined' || expected.type === 'participant.left')
        actual = lifecycle.participant(eventEnvelope(expected), expected.type, authenticatedActor(expected.actor));
      else if (expected.type === 'meeting.connection_lost' || expected.type === 'meeting.connection_recovered')
        actual = lifecycle.connection(eventEnvelope(expected), expected.type, expected.reason);
      else if (expected.type === 'meeting.ended' || expected.type === 'meeting.aborted')
        actual = lifecycle.terminal(eventEnvelope(expected), expected.type, expected.reason);
      else
        actual = lifecycle.authoritativeReady(eventEnvelope(expected), {
          actors: expected.actors.map(authenticatedActor),
          endedAt: expected.endedAt,
          trackCount: expected.trackCount,
          sourceFilesChecksumSha256: expected.sourceFilesChecksumSha256
        });
    }
    assert.deepEqual(actual, expected, `${fixture.name} drifted from the runtime lifecycle builder`);
  }
  const ready = fixture.events[fixture.events.length - 1];
  assert.deepEqual(
    selectCraigKnowledgeEligibleActorIds(ready, fixture.authoritativeTrackActorIds),
    fixture.expectedKnowledgeEligibleActorIds,
    `${fixture.name} drifted from fail-closed knowledge filtering`
  );
}

async function generate(): Promise<void> {
  const [schema, fixtures] = await Promise.all([readFile(path.join(root, schemaFileName)), readFile(path.join(root, fixturesFileName))]);
  const parsedSchema = JSON.parse(schema.toString('utf8')) as ContractSchemaConstants;
  const parsed = JSON.parse(fixtures.toString('utf8')) as CanonicalFixtures;
  assert.equal(parsed.bundleVersion, 2, 'canonical fixture bundle version changed');
  assert.equal(parsed.contract, 'craig-lifecycle-v3', 'canonical fixture contract changed');
  assert.match(String(parsed.producerRevision), /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/);
  assert.equal(parsed.producerCapabilityId, sealedActorRosterCapabilityId, 'fixture capability drifted from runtime');
  assert.equal(parsed.actorSemanticsVersion, actorSemanticsVersion, 'fixture actor semantics drifted from runtime');
  assert.equal(parsedSchema.$defs.envelope.properties.schemaVersion.const, meetingLifecycleV3SchemaVersion, 'schema version drifted from runtime');
  assert.equal(
    parsedSchema.$defs.envelope.properties.producerCapabilityId.const,
    sealedActorRosterCapabilityId,
    'schema capability drifted from runtime'
  );
  assert.equal(
    parsedSchema.$defs.envelope.properties.actorSemanticsVersion.const,
    actorSemanticsVersion,
    'schema actor semantics drifted from runtime'
  );
  assert.equal(parsedSchema.$defs.actors.maxItems, maximumCraigActorRosterSize, 'schema actor bound drifted from runtime');
  for (const fixture of parsed.fixtures) {
    for (const event of fixture.events) validateCraigLifecycleV3Event(event);
    assertFixtureBuiltByRuntime(fixture, parsed.producerRevision);
  }

  const checksums = `${digest(schema)}  ${schemaFileName}\n${digest(fixtures)}  ${fixturesFileName}\n`;
  const bundle = `${digest(Buffer.from(checksums, 'utf8'))}  ${checksumFileName}\n`;
  const write = process.argv.includes('--write');
  if (write) {
    await Promise.all([writeFile(path.join(root, checksumFileName), checksums, 'utf8'), writeFile(path.join(root, bundleFileName), bundle, 'utf8')]);
    return;
  }

  const [committedChecksums, committedBundle] = await Promise.all([
    readFile(path.join(root, checksumFileName), 'utf8'),
    readFile(path.join(root, bundleFileName), 'utf8')
  ]);
  assert.equal(committedChecksums, checksums, 'SHA256SUMS is stale; regenerate with --write');
  assert.equal(committedBundle, bundle, 'BUNDLE.sha256 is stale; regenerate with --write');
}

void generate().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
