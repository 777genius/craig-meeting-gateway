import assert from 'node:assert/strict';
import { test } from 'node:test';

import { classifyDiscordActor, compareOpaqueDiscordIds, RecordingActorRegistry, validateActorRoster } from './meetingActorIdentity';

const humanId = '1533227577286852649';
const botikId = '1533228054724346087';
const applicationId = '1533228590643155034';

test('classifies only positively identified ordinary Discord users as human', () => {
  assert.deepEqual(classifyDiscordActor(humanId, { id: humanId, bot: false }, botikId), { actorId: humanId, kind: 'human' });
  assert.deepEqual(classifyDiscordActor(humanId, { user: { id: humanId, bot: false } }, botikId), {
    actorId: humanId,
    kind: 'human'
  });
});

test('classifies bots, applications, Botik and the Craig recorder as automation', () => {
  assert.deepEqual(classifyDiscordActor(applicationId, { id: applicationId, bot: true }, botikId), {
    actorId: applicationId,
    kind: 'automation'
  });
  assert.deepEqual(classifyDiscordActor(applicationId, { id: applicationId, applicationId }, botikId), {
    actorId: applicationId,
    kind: 'automation'
  });
  assert.deepEqual(classifyDiscordActor(botikId, undefined, botikId), { actorId: botikId, kind: 'automation' });
});

test('classifies missing, partial, mismatched and conflicting Discord identities as unknown', () => {
  for (const identity of [
    undefined,
    {},
    { id: humanId },
    { id: applicationId, bot: false },
    { id: humanId, bot: false, user: { id: humanId, bot: true } }
  ])
    assert.deepEqual(classifyDiscordActor(humanId, identity, botikId), { actorId: humanId, kind: 'unknown' });
});

test('keeps a recording actor kind immutable and returns a stable sorted roster', () => {
  const registry = new RecordingActorRegistry();
  registry.register({ actorId: botikId, kind: 'automation' });
  registry.register({ actorId: humanId, kind: 'human' });
  assert.strictEqual(registry.register({ actorId: humanId, kind: 'human' }), registry.get(humanId));
  assert.throws(() => registry.register({ actorId: humanId, kind: 'unknown' }), /cannot change kind/);
  const roster = registry.roster();
  assert.deepEqual(roster, [
    { actorId: humanId, kind: 'human' },
    { actorId: botikId, kind: 'automation' }
  ]);
  assert.equal(Object.isFrozen(roster), true);
  assert.equal(Object.isFrozen(roster[0]), true);
});

test('sorts opaque Discord IDs deterministically without locale-sensitive comparison', () => {
  const originalLocaleCompare = String.prototype.localeCompare;
  String.prototype.localeCompare = () => {
    throw new Error('localeCompare must not determine the actor wire order');
  };
  try {
    assert.deepEqual([botikId, humanId, applicationId].sort(compareOpaqueDiscordIds), [humanId, botikId, applicationId]);
    const registry = new RecordingActorRegistry();
    registry.register({ actorId: applicationId, kind: 'automation' });
    registry.register({ actorId: botikId, kind: 'automation' });
    registry.register({ actorId: humanId, kind: 'human' });
    assert.deepEqual(
      registry.roster().map(({ actorId }) => actorId),
      [humanId, botikId, applicationId]
    );
  } finally {
    String.prototype.localeCompare = originalLocaleCompare;
  }
});

test('rejects duplicate and conflicting actor roster entries at the contract boundary', () => {
  assert.throws(
    () =>
      validateActorRoster([
        { actorId: humanId, kind: 'human' },
        { actorId: humanId, kind: 'human' }
      ]),
    /repeats actor/
  );
  assert.throws(
    () =>
      validateActorRoster([
        { actorId: humanId, kind: 'human' },
        { actorId: humanId, kind: 'automation' }
      ]),
    /repeats actor/
  );
});
