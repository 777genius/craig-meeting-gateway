const MAX_CHANNELS = 64;
const MAX_SYNTHETIC_BOTS = 128;
const DEFAULT_START_DELAY_MS = 250;
const DEFAULT_EMPTY_GRACE_MS = 5_000;
const MAX_DELAY_MS = 60_000;
const DISCORD_SNOWFLAKE = /^\d{17,20}$/;

export interface MeetingAutoRecordConfig {
  enabled: boolean;
  channelIds?: string[];
  syntheticBotUserIds?: string[];
  startDelayMs?: number;
  emptyGraceMs?: number;
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

export function normalizeMeetingAutoRecordConfig(config: MeetingAutoRecordConfig | undefined): NormalizedMeetingAutoRecordConfig {
  if (config?.enabled !== true)
    return {
      enabled: false,
      channelIds: new Set(),
      syntheticBotUserIds: new Set(),
      startDelayMs: DEFAULT_START_DELAY_MS,
      emptyGraceMs: DEFAULT_EMPTY_GRACE_MS
    };

  const channelIds = normalizeIds('channelIds', config.channelIds, MAX_CHANNELS);
  if (channelIds.size === 0) throw new Error('channelIds must contain at least one channel when Meeting auto-record is enabled');

  return {
    enabled: true,
    channelIds,
    syntheticBotUserIds: normalizeIds('syntheticBotUserIds', config.syntheticBotUserIds, MAX_SYNTHETIC_BOTS),
    startDelayMs: normalizeDelay('startDelayMs', config.startDelayMs, DEFAULT_START_DELAY_MS),
    emptyGraceMs: normalizeDelay('emptyGraceMs', config.emptyGraceMs, DEFAULT_EMPTY_GRACE_MS)
  };
}

export class MeetingAutoRecordCoordinator {
  private readonly ownedRecordingIds = new Map<string, string>();
  private readonly guildOperations = new Map<string, Promise<void>>();
  private readonly timers = new Map<string, NodeJS.Timeout>();

  constructor(
    private readonly config: NormalizedMeetingAutoRecordConfig,
    private readonly selfUserId: string,
    private readonly port: MeetingAutoRecordPort
  ) {}

  isEnabled(): boolean {
    return this.config.enabled;
  }

  configuredChannelIds(): Iterable<string> {
    return this.config.channelIds;
  }

  observesParticipant(participant: MeetingAutoRecordParticipant): boolean {
    return participant.id !== this.selfUserId && (!participant.bot || this.config.syntheticBotUserIds.has(participant.id));
  }

  schedule(guildId: string, channelId: string, possiblyEmpty: boolean): void {
    if (!this.config.enabled || !this.config.channelIds.has(channelId)) return;

    const key = `${guildId}:${channelId}`;
    const existing = this.timers.get(key);
    if (existing) clearTimeout(existing);

    const delayMs = possiblyEmpty ? this.config.emptyGraceMs : this.config.startDelayMs;
    this.timers.set(
      key,
      setTimeout(() => {
        this.timers.delete(key);
        void this.reconcile(guildId, channelId);
      }, delayMs)
    );
  }

  reconcile(guildId: string, channelId: string): Promise<void> {
    if (!this.config.enabled || !this.config.channelIds.has(channelId)) return Promise.resolve();

    const previous = this.guildOperations.get(guildId) ?? Promise.resolve();
    const operation = previous
      .catch(() => undefined)
      .then(async () => this.reconcileOnce(guildId, channelId))
      .finally(() => {
        if (this.guildOperations.get(guildId) === operation) this.guildOperations.delete(guildId);
      });
    this.guildOperations.set(guildId, operation);
    return operation;
  }

  dispose(): void {
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
  }

  private async reconcileOnce(guildId: string, channelId: string): Promise<void> {
    const channel = this.port.getChannel(guildId, channelId);
    if (!channel) return;

    const participants = channel.participants.filter((participant) => this.observesParticipant(participant));
    const active = this.port.getActiveRecording(guildId);
    const ownedRecordingId = this.ownedRecordingIds.get(guildId);

    if (ownedRecordingId && active?.id !== ownedRecordingId) this.ownedRecordingIds.delete(guildId);

    if (active) {
      if (active.id !== ownedRecordingId || active.channelId !== channelId || participants.length > 0) return;
      await this.port.stop(guildId, active.id);
      this.ownedRecordingIds.delete(guildId);
      for (const configuredChannelId of this.config.channelIds) {
        if (configuredChannelId !== channelId) this.schedule(guildId, configuredChannelId, false);
      }
      return;
    }

    if (participants.length === 0) return;
    const owner = participants.find((participant) => !participant.bot) ?? participants[0];
    const started = await this.port.start(guildId, channelId, owner.id);
    if (started) this.ownedRecordingIds.set(guildId, started.id);
  }
}
