import { timingSafeEqual } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';

const DISCORD_API_ORIGIN = 'https://discord.com/api/v10';
const SNOWFLAKE = /^\d{17,20}$/;
const MAX_CHANNELS = 16;

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

async function readSecretSnapshot(path: string): Promise<SecretSnapshot> {
  const before = await stat(path, { bigint: true });
  if (!before.isFile()) throw new DiscordIdentityProofError('secret_not_regular_file');
  const content = await readFile(path);
  const after = await stat(path, { bigint: true });
  const snapshot = (metadata: typeof before): SecretSnapshot => ({
    content,
    device: metadata.dev,
    inode: metadata.ino,
    mode: Number(metadata.mode & 0o777n),
    uid: Number(metadata.uid),
    gid: Number(metadata.gid),
    size: metadata.size,
    modifiedNs: metadata.mtimeNs,
    changedNs: metadata.ctimeNs
  });
  const beforeSnapshot = snapshot(before);
  const afterSnapshot = snapshot(after);
  if (!sameGeneration(beforeSnapshot, afterSnapshot) || BigInt(content.length) !== after.size)
    throw new DiscordIdentityProofError('secret_changed_during_read');
  return afterSnapshot;
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
  if (secret.content.length < 20 || secret.content.length > 256) throw new DiscordIdentityProofError('invalid_secret_size');
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

async function discordGet(fetcher: typeof globalThis.fetch, path: string, token: string): Promise<Record<string, unknown>> {
  const response = await fetcher(`${DISCORD_API_ORIGIN}${path}`, {
    headers: { authorization: `Bot ${token}`, accept: 'application/json' },
    redirect: 'error',
    signal: AbortSignal.timeout(10_000)
  });
  if (!response.ok) throw new DiscordIdentityProofError(`discord_http_${response.status}`);
  const body: unknown = await response.json();
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new DiscordIdentityProofError('invalid_discord_response');
  return body as Record<string, unknown>;
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

  const readSecret = dependencies.readSecret || readSecretSnapshot;
  const fetcher = dependencies.fetch || globalThis.fetch;
  const before = await readSecret(secretPath);
  assertCustody(before);
  const token = before.content.toString('utf8').trim();
  if (!token || /[\r\n\0]/.test(token)) throw new DiscordIdentityProofError('invalid_secret_content');

  const self = await discordGet(fetcher, '/users/@me', token);
  if (!SNOWFLAKE.test(String(self.id || '')) || self.bot !== true) throw new DiscordIdentityProofError('identity_is_not_bot');
  if (self.id !== applicationId) throw new DiscordIdentityProofError('application_identity_mismatch');

  const guild = await discordGet(fetcher, `/guilds/${guildId}`, token);
  if (guild.id !== guildId) throw new DiscordIdentityProofError('guild_identity_mismatch');

  for (const channelId of channelIds) {
    const channel = await discordGet(fetcher, `/channels/${channelId}`, token);
    if (channel.id !== channelId || channel.guild_id !== guildId) throw new DiscordIdentityProofError('channel_identity_mismatch');
  }

  const after = await readSecret(secretPath);
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
}
