const MAX_CHANNELS = 64;
const MAX_SYNTHETIC_BOTS = 128;
const DEFAULT_START_DELAY_MS = 250;
const DEFAULT_EMPTY_GRACE_MS = 5_000;
const DEFAULT_CONFIGURATION_POLL_MS = 5_000;
const MAX_DELAY_MS = 60_000;
const MIN_CONFIGURATION_POLL_MS = 100;
const DISCORD_SNOWFLAKE = /^\d{17,20}$/;

export interface MeetingAutoRecordConfig {
  enabled: boolean;
  channelIds?: string[];
  syntheticBotUserIds?: string[];
  startDelayMs?: number;
  emptyGraceMs?: number;
  platformConfiguration?: boolean;
  configurationPollMs?: number;
}

export interface MeetingAutoRecordParticipant {
  id: string;
  bot: boolean;
}

export interface MeetingAutoRecordChannel {
  id: string;
  guildId: string;
  participants: MeetingAutoRecordParticipant[];
}

export interface MeetingAutoRecordActiveRecording {
  id: string;
  channelId: string;
}

export interface MeetingAutoRecordConfiguredChannel {
  guildId: string;
  channelId: string;
}

export interface MeetingAutoRecordConfigurationUpdate {
  added: readonly MeetingAutoRecordConfiguredChannel[];
  removed: readonly MeetingAutoRecordConfiguredChannel[];
}

export interface MeetingAutoRecordPort {
  getChannel(guildId: string, channelId: string): MeetingAutoRecordChannel | undefined;
  getActiveRecording(guildId: string): MeetingAutoRecordActiveRecording | undefined;
  start(guildId: string, channelId: string, ownerId: string): Promise<MeetingAutoRecordActiveRecording | undefined>;
  stop(guildId: string, recordingId: string): Promise<void>;
}

export interface NormalizedMeetingAutoRecordConfig {
  enabled: boolean;
  channelIds: ReadonlySet<string>;
  syntheticBotUserIds: ReadonlySet<string>;
  startDelayMs: number;
  emptyGraceMs: number;
  platformConfiguration: boolean;
  configurationPollMs: number;
}

function normalizeIds(name: string, ids: string[] | undefined, maximum: number): ReadonlySet<string> {
  const values = ids ?? [];
  if (values.length > maximum) throw new Error(`${name} cannot contain more than ${maximum} entries`);
  if (values.some((id) => !DISCORD_SNOWFLAKE.test(id))) throw new Error(`${name} must contain only Discord snowflake IDs`);
  return new Set(values);
}

function normalizeDelay(name: string, value: number | undefined, fallback: number): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < 0 || result > MAX_DELAY_MS)
    throw new Error(`${name} must be an integer between 0 and ${MAX_DELAY_MS}`);
  return result;
}

function normalizeConfigurationPollMs(value: number | undefined): number {
  const result = value ?? DEFAULT_CONFIGURATION_POLL_MS;
  if (!Number.isSafeInteger(result) || result < MIN_CONFIGURATION_POLL_MS || result > MAX_DELAY_MS)
    throw new Error(`configurationPollMs must be an integer between ${MIN_CONFIGURATION_POLL_MS} and ${MAX_DELAY_MS}`);
  return result;
}

function configuredChannelKey(guildId: string, channelId: string): string {
  return `${guildId}:${channelId}`;
}

function compareIds(left: string, right: string): number {
  return left === right ? 0 : left < right ? -1 : 1;
}

function compareConfiguredChannels(left: MeetingAutoRecordConfiguredChannel, right: MeetingAutoRecordConfiguredChannel): number {
  return compareIds(left.guildId, right.guildId) || compareIds(left.channelId, right.channelId);
}

function normalizePlatformChannels(channels: readonly MeetingAutoRecordConfiguredChannel[]): ReadonlyMap<string, MeetingAutoRecordConfiguredChannel> {
  if (channels.length > MAX_CHANNELS) throw new Error(`platform configuration cannot contain more than ${MAX_CHANNELS} channels`);

  const normalized = new Map<string, MeetingAutoRecordConfiguredChannel>();
  for (const channel of channels) {
    if (!channel || !DISCORD_SNOWFLAKE.test(channel.guildId) || !DISCORD_SNOWFLAKE.test(channel.channelId))
      throw new Error('platform configuration must contain only Discord guild and voice channel snowflake IDs');
    const key = configuredChannelKey(channel.guildId, channel.channelId);
    if (normalized.has(key)) throw new Error('platform configuration cannot contain duplicate guild and voice channel pairs');
    normalized.set(key, { guildId: channel.guildId, channelId: channel.channelId });
  }

  return new Map([...normalized.entries()].sort(([, left], [, right]) => compareConfiguredChannels(left, right)));
}

export function normalizeMeetingAutoRecordConfig(config: MeetingAutoRecordConfig | undefined): NormalizedMeetingAutoRecordConfig {
  if (config?.enabled !== true)
    return {
      enabled: false,
      channelIds: new Set(),
      syntheticBotUserIds: new Set(),
      startDelayMs: DEFAULT_START_DELAY_MS,
      emptyGraceMs: DEFAULT_EMPTY_GRACE_MS,
      platformConfiguration: false,
      configurationPollMs: DEFAULT_CONFIGURATION_POLL_MS
    };

  const channelIds = normalizeIds('channelIds', config.channelIds, MAX_CHANNELS);
  const platformConfiguration = config.platformConfiguration === true;
  if (channelIds.size === 0 && !platformConfiguration)
    throw new Error('channelIds must contain at least one channel when Meeting auto-record is enabled without platformConfiguration');

  return {
    enabled: true,
    channelIds,
    syntheticBotUserIds: normalizeIds('syntheticBotUserIds', config.syntheticBotUserIds, MAX_SYNTHETIC_BOTS),
    startDelayMs: normalizeDelay('startDelayMs', config.startDelayMs, DEFAULT_START_DELAY_MS),
    emptyGraceMs: normalizeDelay('emptyGraceMs', config.emptyGraceMs, DEFAULT_EMPTY_GRACE_MS),
    platformConfiguration,
    configurationPollMs: normalizeConfigurationPollMs(config.configurationPollMs)
  };
}

export class MeetingAutoRecordCoordinator {
  private readonly ownedRecordingIds = new Map<string, string>();
  private readonly guildOperations = new Map<string, Promise<void>>();
  private readonly timers = new Map<string, { guildId: string; channelId: string; timer: NodeJS.Timeout }>();
  // Undefined means that a successful platform snapshot has not arrived yet, so
  // the static allowlist is the fail-closed fallback.
  private platformChannels?: ReadonlyMap<string, MeetingAutoRecordConfiguredChannel>;

  constructor(
    private readonly config: NormalizedMeetingAutoRecordConfig,
    private readonly selfUserId: string,
    private readonly port: MeetingAutoRecordPort
  ) {}

  isEnabled(): boolean {
    return this.config.enabled;
  }

  configuredChannelIds(): Iterable<string> {
    if (!this.platformChannels) return this.config.channelIds;
    return new Set([...this.platformChannels.values()].map(({ channelId }) => channelId));
  }

  configuredChannelIdsForGuild(guildId: string): Iterable<string> {
    if (!this.platformChannels) return this.config.channelIds;
    return [...this.platformChannels.values()].filter((channel) => channel.guildId === guildId).map((channel) => channel.channelId);
  }

  observesParticipant(participant: MeetingAutoRecordParticipant): boolean {
    return participant.id !== this.selfUserId && (!participant.bot || this.config.syntheticBotUserIds.has(participant.id));
  }

  schedule(guildId: string, channelId: string, possiblyEmpty: boolean): void {
    if (!this.isConfigured(guildId, channelId)) return;

    const key = configuredChannelKey(guildId, channelId);
    const existing = this.timers.get(key);
    if (existing) clearTimeout(existing.timer);

    const delayMs = possiblyEmpty ? this.config.emptyGraceMs : this.config.startDelayMs;
    const timer = setTimeout(() => {
      const scheduled = this.timers.get(key);
      if (scheduled?.timer === timer) this.timers.delete(key);
      void this.reconcile(guildId, channelId);
    }, delayMs);
    this.timers.set(key, { guildId, channelId, timer });
  }

  reconcile(guildId: string, channelId: string): Promise<void> {
    if (!this.isConfigured(guildId, channelId)) return Promise.resolve();

    return this.enqueueGuildOperation(guildId, async () => this.reconcileOnce(guildId, channelId));
  }

  async replacePlatformChannelSnapshot(channels: readonly MeetingAutoRecordConfiguredChannel[]): Promise<MeetingAutoRecordConfigurationUpdate> {
    if (!this.config.platformConfiguration) throw new Error('Meeting Platform configuration is not enabled for this auto-record coordinator');
    const nextChannels = normalizePlatformChannels(channels);
    const previousChannels = this.platformChannels;
    const added = [...nextChannels.values()]
      .filter((channel) => !previousChannels?.has(configuredChannelKey(channel.guildId, channel.channelId)))
      .sort(compareConfiguredChannels);
    const removed = [...(previousChannels?.values() ?? [])]
      .filter((channel) => !nextChannels.has(configuredChannelKey(channel.guildId, channel.channelId)))
      .sort(compareConfiguredChannels);

    this.platformChannels = nextChannels;
    this.clearUnconfiguredTimers();

    const addedByGuild = new Map<string, Set<string>>();
    for (const channel of added) {
      const guildChannels = addedByGuild.get(channel.guildId) ?? new Set<string>();
      guildChannels.add(channel.channelId);
      addedByGuild.set(channel.guildId, guildChannels);
    }
    for (const channel of removed) {
      if (!addedByGuild.has(channel.guildId)) addedByGuild.set(channel.guildId, new Set());
    }
    // Revisit every owned recording so a deconfigured active channel is stopped
    // without ever touching a manual recording. This also retries a failed stop
    // on a later successful snapshot.
    for (const guildId of this.ownedRecordingIds.keys()) {
      if (!addedByGuild.has(guildId)) addedByGuild.set(guildId, new Set());
    }

    await Promise.all(
      [...addedByGuild.entries()]
        .sort(([leftGuildId], [rightGuildId]) => compareIds(leftGuildId, rightGuildId))
        .map(([guildId, channelIds]) =>
          this.enqueueGuildOperation(guildId, async () => {
            await this.stopDeconfiguredOwnedRecording(guildId);
            for (const channelId of [...channelIds].sort()) await this.reconcileOnce(guildId, channelId);
          })
        )
    );

    return { added, removed };
  }

  private enqueueGuildOperation(guildId: string, work: () => Promise<void>): Promise<void> {
    const previous = this.guildOperations.get(guildId) ?? Promise.resolve();
    const operation = previous
      .catch(() => undefined)
      .then(work)
      .finally(() => {
        if (this.guildOperations.get(guildId) === operation) this.guildOperations.delete(guildId);
      });
    this.guildOperations.set(guildId, operation);
    return operation;
  }

  dispose(): void {
    for (const { timer } of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
  }

  private async reconcileOnce(guildId: string, channelId: string): Promise<void> {
    if (!this.isConfigured(guildId, channelId)) return;
    const channel = this.port.getChannel(guildId, channelId);
    if (!channel || channel.guildId !== guildId || channel.id !== channelId) return;

    const participants = channel.participants.filter((participant) => this.observesParticipant(participant));
    const active = this.port.getActiveRecording(guildId);
    const ownedRecordingId = this.ownedRecordingIds.get(guildId);

    if (ownedRecordingId && active?.id !== ownedRecordingId) this.ownedRecordingIds.delete(guildId);

    if (active) {
      if (active.id !== ownedRecordingId || active.channelId !== channelId || participants.length > 0) return;
      await this.port.stop(guildId, active.id);
      this.ownedRecordingIds.delete(guildId);
      for (const configuredChannelId of this.configuredChannelIdsForGuild(guildId)) {
        if (configuredChannelId !== channelId) this.schedule(guildId, configuredChannelId, false);
      }
      return;
    }

    if (participants.length === 0) return;
    const owner = participants.find((participant) => !participant.bot) ?? participants[0];
    const started = await this.port.start(guildId, channelId, owner.id);
    if (!started || started.channelId !== channelId) return;

    this.ownedRecordingIds.set(guildId, started.id);
    if (!this.isConfigured(guildId, channelId)) {
      await this.port.stop(guildId, started.id);
      this.ownedRecordingIds.delete(guildId);
    }
  }

  private isConfigured(guildId: string, channelId: string): boolean {
    if (!this.config.enabled) return false;
    return this.platformChannels ? this.platformChannels.has(configuredChannelKey(guildId, channelId)) : this.config.channelIds.has(channelId);
  }

  private clearUnconfiguredTimers(): void {
    for (const [key, scheduled] of this.timers.entries()) {
      if (this.isConfigured(scheduled.guildId, scheduled.channelId)) continue;
      clearTimeout(scheduled.timer);
      this.timers.delete(key);
    }
  }

  private async stopDeconfiguredOwnedRecording(guildId: string): Promise<void> {
    const ownedRecordingId = this.ownedRecordingIds.get(guildId);
    if (!ownedRecordingId) return;

    const active = this.port.getActiveRecording(guildId);
    if (active?.id !== ownedRecordingId) {
      this.ownedRecordingIds.delete(guildId);
      return;
    }
    if (this.isConfigured(guildId, active.channelId)) return;

    await this.port.stop(guildId, active.id);
    this.ownedRecordingIds.delete(guildId);
  }
}
