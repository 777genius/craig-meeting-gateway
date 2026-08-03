import { DexareClient, DexareModule } from 'dexare';
import Eris from 'eris';

import type { CraigBotConfig } from '../bot';
import { parseRewards } from '../util';
import {
  MeetingAutoRecordActiveRecording,
  MeetingAutoRecordChannel,
  MeetingAutoRecordCoordinator,
  normalizeMeetingAutoRecordConfig
} from './meetingAutoRecordCoordinator';
import type RecorderModule from './recorder';
import { type MeetingPlatformConfigurationClient, createMeetingPlatformConfigurationClient } from './recorder/meetingIntegration';
import Recording, { RecordingState } from './recorder/recording';

export type { MeetingAutoRecordConfig } from './meetingAutoRecordCoordinator';

// This adapter is deliberately separate from Craig's premium autorecord module.
// It is a deployment-only allowlist path for private synthetic E2E guilds.
export default class MeetingAutoRecordModule extends DexareModule<DexareClient<CraigBotConfig>> {
  private coordinator?: MeetingAutoRecordCoordinator;
  private platformConfigurationClient?: MeetingPlatformConfigurationClient;
  private configurationPollTimer?: NodeJS.Timeout;
  private configurationPolling = false;
  private platformConfigurationEnabled = false;
  private configurationPollMs = 5_000;
  private disposed = false;

  constructor(client: DexareClient<CraigBotConfig>) {
    super(client, {
      name: 'meeting-autorecord',
      requires: ['recorder'],
      description: 'Allowlisted Meeting Platform auto-record lifecycle'
    });

    this.filePath = __filename;
  }

  get recorder() {
    return this.client.modules.get('recorder') as RecorderModule<DexareClient<CraigBotConfig>>;
  }

  load() {
    const config = normalizeMeetingAutoRecordConfig(this.client.config.craig.meetingAutoRecord);
    if (!config.enabled) {
      this.logger.debug('Meeting auto-record is disabled');
      return;
    }

    this.platformConfigurationEnabled = config.platformConfiguration;
    this.configurationPollMs = config.configurationPollMs;

    // The public application ID is also the bot user ID and is available before
    // the gateway ready event; `bot.user` is intentionally unavailable here.
    this.coordinator = new MeetingAutoRecordCoordinator(config, this.client.config.applicationID, {
      getChannel: (guildId, channelId) => this.getChannel(guildId, channelId),
      getActiveRecording: (guildId) => {
        const recording = this.recorder.recordings.get(guildId);
        return recording ? { id: recording.id, channelId: recording.channel.id } : undefined;
      },
      start: (guildId, channelId, ownerId) => this.startRecording(guildId, channelId, ownerId),
      stop: (guildId, recordingId) => this.stopRecording(guildId, recordingId)
    });

    this.registerEvent('ready', this.onReady.bind(this));
    this.registerEvent('voiceChannelJoin', this.onVoiceChannelJoin.bind(this));
    this.registerEvent('voiceChannelLeave', this.onVoiceChannelLeave.bind(this));
    this.registerEvent('voiceChannelSwitch', this.onVoiceChannelSwitch.bind(this));
    if (this.platformConfigurationEnabled && !this.client.config.craig.meetingIntegration?.enabled) {
      this.platformConfigurationEnabled = false;
      this.logger.warn(
        'Meeting auto-record platform configuration is enabled but Meeting integration is disabled; retaining the static allowlist fallback'
      );
    }
    this.logger.info(
      `Meeting auto-record enabled for ${[...this.coordinator.configuredChannelIds()].length} static fallback channel(s)${
        this.platformConfigurationEnabled ? '; Meeting Platform configuration polling is enabled' : ''
      }`
    );
  }

  unload() {
    this.disposed = true;
    if (this.configurationPollTimer) clearTimeout(this.configurationPollTimer);
    this.configurationPollTimer = undefined;
    this.coordinator?.dispose();
    this.unregisterAllEvents();
  }

  private getChannel(guildId: string, channelId: string): MeetingAutoRecordChannel | undefined {
    const guild = this.client.bot.guilds.get(guildId);
    const channel = guild?.channels.get(channelId);
    if (!channel || (channel.type !== 2 && channel.type !== 13)) return;

    const voiceChannel = channel as Eris.StageChannel | Eris.VoiceChannel;
    return {
      id: channelId,
      guildId,
      participants: [...voiceChannel.voiceMembers.values()].map((member) => ({ id: member.id, bot: member.bot }))
    };
  }

  private async startRecording(guildId: string, channelId: string, ownerId: string): Promise<MeetingAutoRecordActiveRecording | undefined> {
    if (this.recorder.recordings.has(guildId)) return;

    const guild = this.client.bot.guilds.get(guildId);
    const channel = guild?.channels.get(channelId);
    if (!guild || !channel || (channel.type !== 2 && channel.type !== 13)) return;
    if (!channel.permissionsOf(this.client.bot.user.id).has('voiceConnect')) {
      this.logger.warn(`Meeting auto-record cannot connect to allowlisted channel ${channelId}: missing voiceConnect permission`);
      return;
    }

    const member = (channel as Eris.StageChannel | Eris.VoiceChannel).voiceMembers.get(ownerId);
    if (!member || !this.coordinator?.observesParticipant({ id: member.id, bot: member.bot })) return;

    const parsedRewards = parseRewards(this.recorder.client.config, 0, 0);
    if (parsedRewards.rewards.recordHours <= 0) {
      this.logger.warn('Meeting auto-record cannot start because the configured default recording duration is zero');
      return;
    }

    const recording = new Recording(this.recorder, channel as Eris.StageChannel | Eris.VoiceChannel, member.user, true);
    this.recorder.recordings.set(guildId, recording);
    this.logger.info(`Starting configured Meeting auto-record ${recording.id} in ${channelId}`);

    try {
      await recording.start(parsedRewards, false);
    } catch (error) {
      this.logger.error(`Failed to start configured Meeting auto-record ${recording.id}`, error);
      recording.state = RecordingState.ERROR;
      await recording.stop(true).catch(() => undefined);
      return;
    }

    if (recording.state !== RecordingState.RECORDING) return;
    return { id: recording.id, channelId };
  }

  private async stopRecording(guildId: string, recordingId: string): Promise<void> {
    const recording = this.recorder.recordings.get(guildId);
    if (!recording || recording.id !== recordingId) return;

    this.logger.info(`Stopping configured Meeting auto-record ${recording.id}: no eligible participants remain`);
    recording.pushToActivity('Meeting auto-record stopped because the configured channel became empty.');
    await recording.stop();
  }

  private onReady() {
    if (!this.coordinator) return;
    for (const guild of this.client.bot.guilds.values()) {
      for (const channelId of this.coordinator.configuredChannelIdsForGuild(guild.id)) {
        if (guild.channels.has(channelId)) this.coordinator.schedule(guild.id, channelId, false);
      }
    }
    this.schedulePlatformConfigurationPoll(0);
  }

  private onVoiceChannelJoin(_: unknown, member: Eris.Member, channel: Eris.StageChannel | Eris.VoiceChannel) {
    if (!this.coordinator?.observesParticipant({ id: member.id, bot: member.bot })) return;
    this.coordinator.schedule(member.guild.id, channel.id, false);
  }

  private onVoiceChannelLeave(_: unknown, member: Eris.Member, channel: Eris.StageChannel | Eris.VoiceChannel) {
    if (!this.coordinator?.observesParticipant({ id: member.id, bot: member.bot })) return;
    this.coordinator.schedule(member.guild.id, channel.id, true);
  }

  private onVoiceChannelSwitch(
    _: unknown,
    member: Eris.Member,
    newChannel: Eris.StageChannel | Eris.VoiceChannel,
    oldChannel: Eris.StageChannel | Eris.VoiceChannel
  ) {
    if (!this.coordinator?.observesParticipant({ id: member.id, bot: member.bot })) return;
    this.coordinator.schedule(member.guild.id, oldChannel.id, true);
    this.coordinator.schedule(member.guild.id, newChannel.id, false);
  }

  private schedulePlatformConfigurationPoll(delayMs: number): void {
    if (!this.platformConfigurationEnabled || this.disposed || this.configurationPollTimer || this.configurationPolling) return;
    this.configurationPollTimer = setTimeout(() => {
      this.configurationPollTimer = undefined;
      void this.pollPlatformConfiguration();
    }, delayMs);
  }

  private async pollPlatformConfiguration(): Promise<void> {
    if (this.disposed || this.configurationPolling || !this.platformConfigurationEnabled) return;
    this.configurationPolling = true;
    try {
      this.platformConfigurationClient ??= await createMeetingPlatformConfigurationClient(this.client.config.craig.meetingIntegration, {
        debug: (message) => this.logger.debug(message),
        warn: (message) => this.logger.warn(message),
        error: (message, error) => this.logger.error(message, error)
      });
      if (!this.platformConfigurationClient) return;

      const configuration = await this.platformConfigurationClient.getConfiguration();
      if (this.disposed || !this.coordinator) return;

      const update = await this.coordinator.replacePlatformChannelSnapshot(
        configuration.channels.map(({ guildId, voiceChannelId }) => ({ guildId, channelId: voiceChannelId }))
      );
      if (update.added.length || update.removed.length)
        this.logger.info(`Reconciled Meeting Platform auto-record configuration: ${update.added.length} added, ${update.removed.length} removed`);
    } catch (error) {
      if (!this.disposed)
        this.logger.warn(
          'Failed to fetch Meeting Platform auto-record configuration; retaining the last-known-good snapshot or static fallback',
          error
        );
    } finally {
      this.configurationPolling = false;
      this.schedulePlatformConfigurationPoll(this.configurationPollMs);
    }
  }
}
