import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { chmod, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import { createDiscordIdentityProof, DiscordIdentityProofError, readDiscordBotSecretSnapshot, SecretSnapshot } from './discordIdentityProof';
import { runDiscordIdentityProofCommand } from './discordIdentityProofCommand';

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

test('opens the real secret without following symlinks and bounds reads to 256 bytes', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'craig-identity-proof-'));
  const secret = join(directory, 'secret');
  const link = join(directory, 'link');
  try {
    await writeFile(secret, Buffer.alloc(257, 0x61), { mode: 0o400 });
    await symlink(secret, link);
    await assert.rejects(
      readDiscordBotSecretSnapshot(link),
      (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'secret_open_failed'
    );
    await assert.rejects(
      readDiscordBotSecretSnapshot(secret),
      (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'invalid_secret_size'
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('FIFO secret path cannot hold a child command open and emits one canonical line', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'craig-identity-proof-'));
  const fifo = join(directory, 'secret-fifo');
  try {
    await new Promise<void>((resolve, reject) => {
      const mkfifo = spawn('mkfifo', [fifo], { stdio: 'ignore' });
      mkfifo.once('error', reject);
      mkfifo.once('exit', (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
    });

    const fixture = join(__dirname, 'discordIdentityProofFifoFixture.ts');
    const result = await new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
      const child = spawn(process.execPath, [...process.execArgv, fixture, fifo], { stdio: ['ignore', 'pipe', 'pipe'] });
      let stdout = '';
      let stderr = '';
      const timeout = setTimeout(() => {
        child.kill('SIGKILL');
        reject(new Error('FIFO proof child exceeded total deadline'));
      }, 1_000);
      child.stdout.setEncoding('utf8').on('data', (chunk: string) => (stdout += chunk));
      child.stderr.setEncoding('utf8').on('data', (chunk: string) => (stderr += chunk));
      child.once('error', reject);
      child.once('exit', (code) => {
        clearTimeout(timeout);
        resolve({ code, stdout, stderr });
      });
    });

    assert.equal(result.code, 1);
    assert.equal(result.stderr, '');
    assert.deepEqual(result.stdout.split('\n'), ['{"schemaVersion":1,"ok":false,"code":"secret_not_regular_file"}', '']);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('real filesystem snapshot preserves special permission bits for exact custody rejection', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'craig-identity-proof-'));
  const secret = join(directory, 'secret');
  try {
    await writeFile(secret, token, { mode: 0o400 });
    await chmod(secret, 0o4400);
    const actual = await readDiscordBotSecretSnapshot(secret);
    assert.equal(actual.mode, 0o4400);
    await assert.rejects(
      createDiscordIdentityProof(environment, { readSecret: async () => actual, fetch: async () => new Response('{}') }),
      (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'invalid_secret_custody'
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
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

test('rejects oversized Discord bodies from content-length and streaming limits', async () => {
  const declared: typeof fetch = async () => new Response('{}', { headers: { 'content-length': '16385' } });
  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: declared }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'discord_response_too_large'
  );

  const streaming: typeof fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array(16_385));
          controller.close();
        }
      })
    );
  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: streaming }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'discord_response_too_large'
  );
});

test('one total proof deadline is propagated to and cancels a pending request', async () => {
  let receivedSignal: AbortSignal | undefined;
  const pending: typeof fetch = async (_input, init) => {
    receivedSignal = init?.signal || undefined;
    return await new Promise<Response>((_resolve, reject) => {
      receivedSignal?.addEventListener('abort', () => reject(receivedSignal?.reason), { once: true });
    });
  };

  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: pending, timeoutMs: 10 }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'proof_timeout'
  );
  assert.equal(receivedSignal?.aborted, true);
});

test('pre-aborted proof does not start secret reads or Discord requests', async () => {
  const controller = new AbortController();
  controller.abort();
  let secretReads = 0;
  let requests = 0;
  const lines: string[] = [];

  const exitCode = await runDiscordIdentityProofCommand(
    environment,
    (line) => lines.push(line),
    (commandEnvironment) =>
      createDiscordIdentityProof(commandEnvironment, {
        signal: controller.signal,
        readSecret: async () => {
          secretReads += 1;
          return snapshot();
        },
        fetch: async () => {
          requests += 1;
          return new Response('{}');
        }
      })
  );

  assert.equal(secretReads, 0);
  assert.equal(requests, 0);
  assert.equal(exitCode, 1);
  assert.deepEqual(lines, ['{"schemaVersion":1,"ok":false,"code":"proof_cancelled"}\n']);
});

test('one total proof deadline also cancels a stalled response body', async () => {
  const stalled: typeof fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('{'));
        }
      })
    );

  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: stalled, timeoutMs: 10 }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'proof_timeout'
  );
});

test('stalled read with never-settling cancellation cannot hold proof or CLI past the total deadline', async () => {
  let cancelCalled = false;
  const stalled: typeof fetch = async () =>
    new Response(
      new ReadableStream({
        pull: () => new Promise<void>(() => undefined),
        cancel: () => {
          cancelCalled = true;
          return new Promise<void>(() => undefined);
        }
      })
    );

  const startedAt = Date.now();
  const lines: string[] = [];
  const exitCode = await runDiscordIdentityProofCommand(
    environment,
    (line) => lines.push(line),
    (commandEnvironment) => createDiscordIdentityProof(commandEnvironment, { readSecret: async () => snapshot(), fetch: stalled, timeoutMs: 10 })
  );

  assert.equal(exitCode, 1);
  assert.ok(Date.now() - startedAt < 1_000);
  assert.equal(cancelCalled, true);
  assert.deepEqual(lines, ['{"schemaVersion":1,"ok":false,"code":"proof_timeout"}\n']);
});

test('non-2xx responses terminate stalled bodies and abort the request lifecycle', async () => {
  let cancelCalled = false;
  let requestSignal: AbortSignal | undefined;
  const rejected: typeof fetch = async (_input, init) => {
    requestSignal = init?.signal || undefined;
    return new Response(
      new ReadableStream({
        pull: () => new Promise<void>(() => undefined),
        cancel: () => {
          cancelCalled = true;
          return new Promise<void>(() => undefined);
        }
      }),
      { status: 401 }
    );
  };

  await assert.rejects(
    createDiscordIdentityProof(environment, { readSecret: async () => snapshot(), fetch: rejected }),
    (error: unknown) => error instanceof DiscordIdentityProofError && error.code === 'discord_http_401'
  );
  assert.equal(cancelCalled, true);
  assert.equal(requestSignal?.aborted, true);
});

test('command emits exactly one canonical bounded JSON line for filesystem failures', async () => {
  const lines: string[] = [];
  const exitCode = await runDiscordIdentityProofCommand(
    environment,
    (line) => lines.push(line),
    async () => {
      throw new DiscordIdentityProofError('secret_open_failed');
    }
  );

  assert.equal(exitCode, 1);
  assert.deepEqual(lines, ['{"schemaVersion":1,"ok":false,"code":"secret_open_failed"}\n']);
  assert.ok(lines[0].length < 128);
});

test('entrypoint delegates proof failures directly to the canonical CLI', async () => {
  const entrypoint = await readFile(join(process.cwd(), 'deploy/meeting/entrypoint.sh'), 'utf8');
  const proofCase = entrypoint.match(/discord-identity-proof\)\n([\s\S]*?)\n {4};;/)?.[1] || '';
  assert.doesNotMatch(proofCase, /require_secret_file|echo|stderr/);
  assert.match(proofCase, /exec node .*discordIdentityProofCli\.js/);
});
