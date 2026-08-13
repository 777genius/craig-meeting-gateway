import { timingSafeEqual } from 'node:crypto';
import type { BigIntStats } from 'node:fs';
import { constants } from 'node:fs';
import { open } from 'node:fs/promises';

const DISCORD_API_ORIGIN = 'https://discord.com/api/v10';
const SNOWFLAKE = /^\d{17,20}$/;
const MAX_CHANNELS = 16;
const MAX_SECRET_BYTES = 256;
const MAX_DISCORD_BODY_BYTES = 16 * 1024;
const DEFAULT_PROOF_TIMEOUT_MS = 10_000;

export type SecretSnapshot = {
  content: Buffer;
  device: bigint;
  inode: bigint;
  mode: number;
  uid: number;
  gid: number;
  size: bigint;
  modifiedNs: bigint;
  changedNs: bigint;
};

export type DiscordIdentityProofDependencies = {
  readSecret?: (path: string) => Promise<SecretSnapshot>;
  fetch?: typeof globalThis.fetch;
  signal?: AbortSignal;
  timeoutMs?: number;
};

export type DiscordIdentityProof = {
  schemaVersion: 1;
  ok: true;
  bot: { id: string; bot: true };
  target: { testOnly: true; guildId: string; channelIds: string[] };
  secret: { path: '/run/secrets/discord_bot_token'; uid: 10001; gid: 10001; mode: '0400'; stable: true };
};

export class DiscordIdentityProofError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = 'DiscordIdentityProofError';
  }
}

function snapshot(content: Buffer, metadata: BigIntStats): SecretSnapshot {
  return {
    content,
    device: metadata.dev,
    inode: metadata.ino,
    mode: Number(metadata.mode & 0o7777n),
    uid: Number(metadata.uid),
    gid: Number(metadata.gid),
    size: metadata.size,
    modifiedNs: metadata.mtimeNs,
    changedNs: metadata.ctimeNs
  };
}

export async function readDiscordBotSecretSnapshot(path: string): Promise<SecretSnapshot> {
  let handle: Awaited<ReturnType<typeof open>>;
  try {
    handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  } catch {
    throw new DiscordIdentityProofError('secret_open_failed');
  }

  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile()) throw new DiscordIdentityProofError('secret_not_regular_file');
    if (before.size < 1n || before.size > BigInt(MAX_SECRET_BYTES)) throw new DiscordIdentityProofError('invalid_secret_size');

    const content = Buffer.alloc(Number(before.size));
    let offset = 0;
    while (offset < content.length) {
      const { bytesRead } = await handle.read(content, offset, content.length - offset, offset);
      if (bytesRead === 0) break;
      offset += bytesRead;
    }
    const overflow = Buffer.alloc(1);
    const { bytesRead: overflowBytes } = await handle.read(overflow, 0, 1, offset);
    const after = await handle.stat({ bigint: true });
    const beforeSnapshot = snapshot(content, before);
    const afterSnapshot = snapshot(content, after);
    if (offset !== content.length || overflowBytes !== 0 || !sameGeneration(beforeSnapshot, afterSnapshot))
      throw new DiscordIdentityProofError('secret_changed_during_read');
    return afterSnapshot;
  } catch (error) {
    if (error instanceof DiscordIdentityProofError) throw error;
    throw new DiscordIdentityProofError('secret_read_failed');
  } finally {
    await handle.close().catch(() => undefined);
  }
}

function exactSnowflake(name: string, value: string | undefined): string {
  if (!value || !SNOWFLAKE.test(value)) throw new DiscordIdentityProofError(`invalid_${name}`);
  return value;
}

function exactChannelIds(value: string | undefined): string[] {
  const values = (value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
  if (!values.length || values.length > MAX_CHANNELS || new Set(values).size !== values.length || values.some((item) => !SNOWFLAKE.test(item)))
    throw new DiscordIdentityProofError('invalid_channel_ids');
  return values;
}

function assertCustody(secret: SecretSnapshot): void {
  if (secret.uid !== 10001 || secret.gid !== 10001 || secret.mode !== 0o400) throw new DiscordIdentityProofError('invalid_secret_custody');
  if (secret.content.length < 20 || secret.content.length > MAX_SECRET_BYTES) throw new DiscordIdentityProofError('invalid_secret_size');
}

function sameGeneration(left: SecretSnapshot, right: SecretSnapshot): boolean {
  return (
    left.device === right.device &&
    left.inode === right.inode &&
    left.uid === right.uid &&
    left.gid === right.gid &&
    left.mode === right.mode &&
    left.size === right.size &&
    left.modifiedNs === right.modifiedNs &&
    left.changedNs === right.changedNs
  );
}

function sameContent(left: Buffer, right: Buffer): boolean {
  if (left.length !== right.length) return false;
  return timingSafeEqual(left, right);
}

async function abortable<T>(start: () => Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw signal.reason;
  const operation = start();
  return await new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

function cancelBody(body: ReadableStream<Uint8Array> | null, reader?: ReadableStreamDefaultReader<Uint8Array>): void {
  let cancellation: Promise<void>;
  try {
    cancellation = reader ? reader.cancel() : body ? body.cancel() : Promise.resolve();
  } catch {
    return;
  }
  void cancellation.catch(() => undefined);
}

async function readBoundedJson(response: Response, signal: AbortSignal): Promise<Record<string, unknown>> {
  const declaredLength = response.headers.get('content-length');
  if (declaredLength !== null) {
    const length = Number(declaredLength);
    if (!Number.isSafeInteger(length) || length < 0 || length > MAX_DISCORD_BODY_BYTES)
      throw new DiscordIdentityProofError('discord_response_too_large');
  }
  if (!response.body) throw new DiscordIdentityProofError('invalid_discord_response');

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    let done = false;
    while (!done) {
      if (signal.aborted) throw signal.reason;
      const result = await abortable(() => reader.read(), signal);
      done = result.done;
      if (done) break;
      const value = result.value;
      if (!value) throw new DiscordIdentityProofError('invalid_discord_response');
      length += value.byteLength;
      if (length > MAX_DISCORD_BODY_BYTES) throw new DiscordIdentityProofError('discord_response_too_large');
      chunks.push(value);
    }
  } finally {
    cancelBody(response.body, reader);
  }

  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  let body: unknown;
  try {
    body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  } catch {
    throw new DiscordIdentityProofError('invalid_discord_response');
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new DiscordIdentityProofError('invalid_discord_response');
  return body as Record<string, unknown>;
}

async function discordGet(fetcher: typeof globalThis.fetch, path: string, token: string, signal: AbortSignal): Promise<Record<string, unknown>> {
  try {
    const response = await abortable(
      () =>
        fetcher(`${DISCORD_API_ORIGIN}${path}`, {
          headers: { authorization: `Bot ${token}`, accept: 'application/json' },
          redirect: 'error',
          signal
        }),
      signal
    );
    if (!response.ok) {
      cancelBody(response.body);
      throw new DiscordIdentityProofError(`discord_http_${response.status}`);
    }
    return await readBoundedJson(response, signal);
  } catch (error) {
    if (signal.aborted && signal.reason instanceof DiscordIdentityProofError) throw signal.reason;
    if (error instanceof DiscordIdentityProofError) throw error;
    throw new DiscordIdentityProofError('discord_request_failed');
  }
}

function proofSignal(
  externalSignal: AbortSignal | undefined,
  timeoutMs: number
): {
  signal: AbortSignal;
  dispose: () => void;
} {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60_000) throw new DiscordIdentityProofError('invalid_proof_timeout');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new DiscordIdentityProofError('proof_timeout')), timeoutMs);
  timeout.unref();
  const onAbort = () => controller.abort(new DiscordIdentityProofError('proof_cancelled'));
  if (externalSignal?.aborted) onAbort();
  else externalSignal?.addEventListener('abort', onAbort, { once: true });
  return {
    signal: controller.signal,
    dispose: () => {
      clearTimeout(timeout);
      externalSignal?.removeEventListener('abort', onAbort);
      if (!controller.signal.aborted) controller.abort(new DiscordIdentityProofError('proof_finished'));
    }
  };
}

export async function createDiscordIdentityProof(
  environment: NodeJS.ProcessEnv,
  dependencies: DiscordIdentityProofDependencies = {}
): Promise<DiscordIdentityProof> {
  if (environment.CRAIG_E2E_TEST_ONLY !== 'true') throw new DiscordIdentityProofError('test_only_ack_required');
  const applicationId = exactSnowflake('application_id', environment.DISCORD_APPLICATION_ID);
  const guildId = exactSnowflake('guild_id', environment.CRAIG_E2E_DISCORD_GUILD_ID);
  const channelIds = exactChannelIds(environment.CRAIG_E2E_DISCORD_CHANNEL_IDS);
  const secretPath = environment.DISCORD_BOT_TOKEN_FILE || '/run/secrets/discord_bot_token';
  if (secretPath !== '/run/secrets/discord_bot_token') throw new DiscordIdentityProofError('unexpected_secret_path');

  const deadline = proofSignal(dependencies.signal, dependencies.timeoutMs ?? DEFAULT_PROOF_TIMEOUT_MS);
  try {
    const readSecret = dependencies.readSecret || readDiscordBotSecretSnapshot;
    const fetcher = dependencies.fetch || globalThis.fetch;
    const before = await abortable(() => readSecret(secretPath), deadline.signal);
    assertCustody(before);
    const token = before.content.toString('utf8').trim();
    if (!token || /[\r\n\0]/.test(token)) throw new DiscordIdentityProofError('invalid_secret_content');

    const self = await discordGet(fetcher, '/users/@me', token, deadline.signal);
    if (!SNOWFLAKE.test(String(self.id || '')) || self.bot !== true) throw new DiscordIdentityProofError('identity_is_not_bot');
    if (self.id !== applicationId) throw new DiscordIdentityProofError('application_identity_mismatch');

    const guild = await discordGet(fetcher, `/guilds/${guildId}`, token, deadline.signal);
    if (guild.id !== guildId) throw new DiscordIdentityProofError('guild_identity_mismatch');

    for (const channelId of channelIds) {
      const channel = await discordGet(fetcher, `/channels/${channelId}`, token, deadline.signal);
      if (channel.id !== channelId || channel.guild_id !== guildId) throw new DiscordIdentityProofError('channel_identity_mismatch');
    }

    const after = await abortable(() => readSecret(secretPath), deadline.signal);
    assertCustody(after);
    if (!sameGeneration(before, after) || !sameContent(before.content, after.content))
      throw new DiscordIdentityProofError('secret_changed_during_proof');

    return {
      schemaVersion: 1,
      ok: true,
      bot: { id: String(self.id), bot: true },
      target: { testOnly: true, guildId, channelIds },
      secret: { path: '/run/secrets/discord_bot_token', uid: 10001, gid: 10001, mode: '0400', stable: true }
    };
  } finally {
    deadline.dispose();
  }
}
