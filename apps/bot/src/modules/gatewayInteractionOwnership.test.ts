import assert from 'node:assert/strict';
import { test } from 'node:test';

import { forwardOwnedCraigGatewayInteraction } from './gatewayInteractionOwnership';

const commands = [
  { commandName: 'join', type: 1 },
  { commandName: 'guild-only', type: 1, guildIDs: ['craig-guild'] }
];

test('does not forward a Meeting Platform command to Craig slash-create', () => {
  const forwarded: unknown[] = [];

  forwardOwnedCraigGatewayInteraction(
    {
      t: 'INTERACTION_CREATE',
      d: {
        type: 2,
        data: { id: 'platform-command-id', name: 'setup-voice-bot', type: 1 }
      }
    },
    commands,
    (interaction) => forwarded.push(interaction)
  );

  assert.deepEqual(forwarded, []);
});

test('forwards Craig commands, autocomplete, and existing component interactions', () => {
  const forwarded: unknown[] = [];
  const forward = (d: unknown) =>
    forwardOwnedCraigGatewayInteraction({ t: 'INTERACTION_CREATE', d }, commands, (interaction) => forwarded.push(interaction));

  forward({ type: 2, data: { id: 'craig-command-id', name: 'join', type: 1 } });
  forward({ type: 4, data: { id: 'craig-command-id', name: 'join', type: 1 } });
  forward({ type: 3, data: { custom_id: 'rec:recording-1:stop' } });

  assert.equal(forwarded.length, 3);
});

test('does not forward an unowned command autocomplete or a command scoped to another guild', () => {
  const forwarded: unknown[] = [];
  const forward = (d: unknown) =>
    forwardOwnedCraigGatewayInteraction({ t: 'INTERACTION_CREATE', d }, commands, (interaction) => forwarded.push(interaction));

  forward({ type: 4, data: { id: 'platform-command-id', name: 'setup-voice-bot', type: 1 } });
  forward({ type: 2, guild_id: 'other-guild', data: { id: 'guild-command-id', name: 'guild-only', type: 1 } });
  forward({ type: 2, guild_id: 'craig-guild', data: { id: 'guild-command-id', name: 'guild-only', type: 1 } });

  assert.equal(forwarded.length, 1);
});
