import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { test } from 'node:test';

import {
  type AuthoritativeRecordingReadyEvent,
  type MeetingLifecycleEvent,
  parseAuthoritativeRecordingReadyEventV2,
  parseMeetingLifecycleEventV2
} from './meetingIntegration';
import { compareOpaqueDiscordIds } from './meetingActorIdentity';

const contractRoot = path.resolve(__dirname, '../../../../../contracts/craig-lifecycle-v2');

test('exports byte-pinned cross-repository lifecycle v2 schema and canonical fixtures', async () => {
  const sumsBytes = await readFile(path.join(contractRoot, 'SHA256SUMS'));
  const bundlePin = (await readFile(path.join(contractRoot, 'BUNDLE.sha256'), 'utf8')).trim();
  assert.equal(bundlePin, `${createHash('sha256').update(sumsBytes).digest('hex')}  SHA256SUMS`);
  const sums = sumsBytes.toString('utf8').trim().split('\n');
  assert.equal(sums.length, 2);
  for (const line of sums) {
    const match = /^([0-9a-f]{64})  ([A-Za-z0-9.-]+)$/.exec(line);
    assert.ok(match);
    const bytes = await readFile(path.join(contractRoot, match[2]));
    assert.equal(createHash('sha256').update(bytes).digest('hex'), match[1]);
  }

  const schema = JSON.parse(await readFile(path.join(contractRoot, 'craig-lifecycle-v2.schema.json'), 'utf8')) as any;
  assert.equal(schema.$schema, 'https://json-schema.org/draft/2020-12/schema');
  assert.deepEqual(schema.$defs.actor.properties.kind.enum, ['human', 'automation', 'unknown']);
  assert.equal(schema.$defs.envelope.properties.schemaVersion.const, 2);
});

test('canonical producer fixtures satisfy consumer v2 identity and cumulative-roster semantics', async () => {
  const fixture = JSON.parse(await readFile(path.join(contractRoot, 'canonical-fixtures.json'), 'utf8')) as {
    events: Array<MeetingLifecycleEvent | AuthoritativeRecordingReadyEvent>;
    authoritativeTrackActorIds: string[];
    identityLabels: Record<string, string>;
  };
  const authoritativeReady = fixture.events.find(
    (event): event is AuthoritativeRecordingReadyEvent => event.type === 'recording.authoritative_ready'
  );
  assert.ok(authoritativeReady);

  const cumulative = new Map<string, string>();
  for (const event of fixture.events) {
    assert.equal(event.schemaVersion, 2);
    if (event.type === 'recording.authoritative_ready') {
      assert.equal(JSON.stringify(parseAuthoritativeRecordingReadyEventV2(event)), JSON.stringify(event));
      continue;
    }
    const parsed = parseMeetingLifecycleEventV2(event as MeetingLifecycleEvent);
    assert.equal(JSON.stringify(parsed), JSON.stringify(event));
    if (parsed.type === 'meeting.started') for (const actor of parsed.actors ?? []) cumulative.set(actor.actorId, actor.kind);
    if (parsed.type === 'participant.joined' && parsed.actor) cumulative.set(parsed.actor.actorId, parsed.actor.kind);
  }
  assert.deepEqual(
    authoritativeReady.actors.map(({ actorId, kind }) => [actorId, kind]),
    [...cumulative.entries()].sort(([left], [right]) => compareOpaqueDiscordIds(left, right))
  );
  assert.deepEqual(
    authoritativeReady.actors.map(({ actorId }) => actorId),
    fixture.authoritativeTrackActorIds
  );
  assert.match(fixture.identityLabels['1533228054724346087'], /Botik.*Craig-recorder/);
  assert.equal(authoritativeReady.actors.find(({ actorId }) => actorId === '1533228590643155035')?.kind, 'unknown');
});
