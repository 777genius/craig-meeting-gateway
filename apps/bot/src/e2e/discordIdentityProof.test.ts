import assert from 'node:assert/strict';
import { test } from 'node:test';

import { createDiscordIdentityProof, DiscordIdentityProofError, SecretSnapshot } from './discordIdentityProof';

const guildId = '1533228590643155034';
const channelIds = ['1533228823045214398', '1533228823045214399'];
const botId = '1533224474609057793';
const token = Buffer.from('synthetic.discord.bot.token');

function snapshot(content = token, overrides: Partial<SecretSnapshot> = {}): SecretSnapshot {
  return {
    content,
    device: 1n,
    inode: 2n,
    mode: 0o400,
    uid: 10001,
    gid: 10001,
    size: BigInt(content.length),
    modifiedNs: 3n,
    changedNs: 4n,
    ...overrides
  };
}

const environment = {
  CRAIG_E2E_TEST_ONLY: 'true',
  DISCORD_APPLICATION_ID: botId,
  CRAIG_E2E_DISCORD_GUILD_ID: guildId,
  CRAIG_E2E_DISCORD_CHANNEL_IDS: channelIds.join(','),
  DISCORD_BOT_TOKEN_FILE: '/run/secrets/discord_bot_token'
};

test('proves only the exact synthetic target and emits bounded non-secret output', async () => {
  const requests: Array<{ url: string; authorization: string | null }> = [];
  const responses: Record<string, unknown> = {
    '/users/@me': { id: botId, bot: true, username: 'synthetic-craig' },
    [`/guilds/${guildId}`]: { id: guildId, name: 'private-test-guild' },
    [`/channels/${channelIds[0]}`]: { id: channelIds[0], guild_id: guildId, name: 'synthetic-audio-a' },
    [`/channels/${channelIds[1]}`]: { id: channelIds[1], guild_id: guildId, name: 'synthetic-audio-b' }
  };
  const fetcher: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    const apiPath = url.pathname.replace('/api/v10', '');
    requests.push({ url: url.toString(), authorization: new Headers(init?.headers).get('authorization') });
    return new Response(JSON.stringify(responses[apiPath]), { status: 200 });
  };

  const proof = await createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: fetcher });

  assert.deepEqual(
    requests.map((request) => request.url),
    [
      'https://discord.com/api/v10/users/@me',
      `https://discord.com/api/v10/guilds/${guildId}`,
      ...channelIds.map((channelId) => `https://discord.com/api/v10/channels/${channelId}`)
    ]
  );
  assert.ok(requests.every((request) => request.authorization === `Bot ${token.toString()}`));
  assert.deepEqual(proof, {
    schemaVersion: 1,
    ok: true,
    bot: { id: botId, bot: true },
    target: { testOnly: true, guildId, channelIds },
    secret: { path: '/run/secrets/discord_bot_token', uid: 10001, gid: 10001, mode: '0400', stable: true }
  });
  assert.equal(JSON.stringify(proof).includes(token.toString()), false);
});

test('fails closed before network access without explicit test-only target acknowledgement', async () => {
  let called = false;
  await assert.rejects(
    createDiscordIdentityProof(
      { ...environment, CRAIG_E2E_TEST_ONLY: 'false' },
      {
        readSecret: async () => snapshot(),
        fetch: async () => {
          called = true;
          throw new Error('must not run');
        }
      }
    ),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'test_only_ack_required'
  );
  assert.equal(called, false);
});

test('rejects incorrect secret custody before sending a credential', async () => {
  let called = false;
  await assert.rejects(
    createDiscordIdentityProof(environment, {
      readSecret: async () => snapshot(token, { uid: 0, mode: 0o444 }),
      fetch: async () => {
        called = true;
        throw new Error('must not run');
      }
    }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'invalid_secret_custody'
  );
  assert.equal(called, false);
});

test('rejects a valid bot token for a different configured application', async () => {
  const fetcher: typeof fetch = async () => new Response(JSON.stringify({ id: '1533224474609057999', bot: true }));

  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: fetcher }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'application_identity_mismatch'
  );
});

test('detects secret generation and content changes after Discord verification', async () => {
  const reads = [snapshot(), snapshot(Buffer.from('synthetic.discord.bot.other'), { inode: 9n })];
  const fetcher: typeof fetch = async (input) => {
    const path = new URL(String(input)).pathname.replace('/api/v10', '');
    if (path === '/users/@me') return new Response(JSON.stringify({ id: botId, bot: true }));
    if (path === `/guilds/${guildId}`) return new Response(JSON.stringify({ id: guildId }));
    const channelId = path.split('/').pop();
    return new Response(JSON.stringify({ id: channelId, guild_id: guildId }));
  };

  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => reads.shift()!, fetch: fetcher }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'secret_changed_during_proof'
  );
});

test('rejects a channel outside the declared private test guild', async () => {
  const fetcher: typeof fetch = async (input) => {
    const path = new URL(String(input)).pathname.replace('/api/v10', '');
    if (path === '/users/@me') return new Response(JSON.stringify({ id: botId, bot: true }));
    if (path === `/guilds/${guildId}`) return new Response(JSON.stringify({ id: guildId }));
    return new Response(JSON.stringify({ id: path.split('/').pop(), guild_id: '1533228590643155999' }));
  };

  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: fetcher }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'channel_identity_mismatch'
  );
});
