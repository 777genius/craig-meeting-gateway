import { readFile } from 'node:fs/promises';
import fetch from 'node-fetch';

export interface MeetingIntegrationConfig {
  enabled: boolean;
  endpoint: string;
  tokenFile: string;
  maxQueuedPackets?: number;
  batchSize?: number;
  requestTimeoutMs?: number;
}

export interface MeetingLifecycleEvent {
  schemaVersion: 1;
  eventId: string;
  recordingId: string;
  guildId: string;
  channelId: string;
  occurredAt: string;
  type:
    | 'meeting.started'
    | 'participant.joined'
    | 'participant.left'
    | 'meeting.connection_lost'
    | 'meeting.connection_recovered'
    | 'meeting.ended'
    | 'meeting.aborted';
  participantIds?: string[];
  participantId?: string;
  reason?: string | null;
}

export interface MeetingVoicePacket {
  schemaVersion: 1;
  recordingId: string;
  guildId: string;
  channelId: string;
  speakerId: string;
  rtpTimestamp: number;
  rtpSequence: number;
  receivedAtMs: number;
  relativeTimeMs: number;
}

interface WireVoicePacket extends MeetingVoicePacket {
  opusBase64: string;
}

type QueueItem = { type: 'lifecycle'; event: MeetingLifecycleEvent } | { type: 'voice'; packet: WireVoicePacket };

export interface MeetingIntegrationLogger {
  debug(message: string): void;
  error(message: string, error?: unknown): void;
  warn(message: string): void;
}

export interface MeetingIntegrationTransport {
  post(path: '/v1/craig/events' | '/v1/craig/voice-packets', body: unknown): Promise<void>;
}

export interface MeetingIntegrationSink {
  publishLifecycle(event: MeetingLifecycleEvent): boolean;
  publishPacket(packet: MeetingVoicePacket, opus: Buffer): boolean;
  drain(timeoutMs: number): Promise<boolean>;
}

export type MeetingTerminalLifecycleType = Extract<MeetingLifecycleEvent['type'], 'meeting.ended' | 'meeting.aborted'>;

export class MeetingTerminalLifecycle {
  private published = false;

  async complete(
    expectedType: MeetingTerminalLifecycleType,
    finalizeRecording: () => Promise<void>,
    publish: (type: MeetingTerminalLifecycleType) => void
  ): Promise<void> {
    try {
      await finalizeRecording();
    } catch (error) {
      this.abort(publish);
      throw error;
    }

    this.publishOnce(expectedType, publish);
  }

  abort(publish: (type: MeetingTerminalLifecycleType) => void): void {
    this.publishOnce('meeting.aborted', publish);
  }

  private publishOnce(type: MeetingTerminalLifecycleType, publish: (type: MeetingTerminalLifecycleType) => void): void {
    if (this.published) return;
    this.published = true;
    publish(type);
  }
}

export class NoopMeetingIntegrationSink implements MeetingIntegrationSink {
  publishLifecycle(): boolean {
    return true;
  }

  publishPacket(): boolean {
    return true;
  }

  async drain(): Promise<boolean> {
    return true;
  }
}

export class BoundedMeetingIntegrationSink implements MeetingIntegrationSink {
  private readonly queue: QueueItem[] = [];
  private readonly drainWaiters = new Set<() => void>();
  private processing = false;
  private retryTimer: NodeJS.Timeout | null = null;
  private queuedPackets = 0;
  private consecutiveFailures = 0;

  constructor(
    private readonly transport: MeetingIntegrationTransport,
    private readonly logger: MeetingIntegrationLogger,
    private readonly maxQueuedPackets = 8192,
    private readonly batchSize = 128
  ) {
    if (!Number.isSafeInteger(maxQueuedPackets) || maxQueuedPackets < 1) throw new Error('maxQueuedPackets must be a positive integer');
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > maxQueuedPackets)
      throw new Error('batchSize must be a positive integer no greater than maxQueuedPackets');
  }

  publishLifecycle(event: MeetingLifecycleEvent): boolean {
    // Lifecycle traffic is tiny and must remain ordered with the accepted audio.
    if (this.queue.length - this.queuedPackets >= 1024) return false;
    this.queue.push({ type: 'lifecycle', event });
    this.scheduleProcessing();
    return true;
  }

  publishPacket(packet: MeetingVoicePacket, opus: Buffer): boolean {
    if (this.queuedPackets >= this.maxQueuedPackets) return false;

    // Clone only after synchronous bounded admission, never before it.
    const opusBase64 = Buffer.from(opus).toString('base64');
    this.queue.push({ type: 'voice', packet: { ...packet, opusBase64 } });
    this.queuedPackets++;
    this.scheduleProcessing();
    return true;
  }

  async drain(timeoutMs: number): Promise<boolean> {
    if (this.queue.length === 0 && !this.processing) return true;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) throw new Error('timeoutMs must be a non-negative integer');

    return await new Promise<boolean>((resolve) => {
      let settled = false;
      const finish = (drained: boolean) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        this.drainWaiters.delete(onDrained);
        resolve(drained);
      };
      const onDrained = () => finish(true);
      const timeout = setTimeout(() => finish(false), timeoutMs);
      this.drainWaiters.add(onDrained);
      this.scheduleProcessing();
    });
  }

  private scheduleProcessing(delayMs = 0) {
    if (this.processing || this.retryTimer || this.queue.length === 0) return;
    if (delayMs > 0) {
      this.retryTimer = setTimeout(() => {
        this.retryTimer = null;
        void this.process();
      }, delayMs);
      return;
    }
    void this.process();
  }

  private async process() {
    if (this.processing || this.queue.length === 0) return;
    this.processing = true;

    try {
      const first = this.queue[0];
      if (first.type === 'lifecycle') {
        await this.transport.post('/v1/craig/events', first.event);
        this.queue.shift();
      } else {
        const batch: WireVoicePacket[] = [];
        for (const item of this.queue) {
          if (item.type !== 'voice' || batch.length >= this.batchSize) break;
          batch.push(item.packet);
        }
        await this.transport.post('/v1/craig/voice-packets', { schemaVersion: 1, packets: batch });
        this.queue.splice(0, batch.length);
        this.queuedPackets -= batch.length;
      }
      this.consecutiveFailures = 0;
    } catch (error) {
      this.consecutiveFailures++;
      const delayMs = Math.min(10_000, 100 * 2 ** Math.min(this.consecutiveFailures - 1, 7));
      this.logger.error(`Meeting integration delivery failed; retrying in ${delayMs}ms`, error);
      this.processing = false;
      this.scheduleProcessing(delayMs);
      return;
    }

    this.processing = false;
    if (this.queue.length === 0) {
      for (const waiter of this.drainWaiters) waiter();
      this.drainWaiters.clear();
    } else this.scheduleProcessing();
  }
}

class HttpMeetingIntegrationTransport implements MeetingIntegrationTransport {
  constructor(private readonly endpoint: URL, private readonly token: string, private readonly requestTimeoutMs: number) {}

  async post(path: '/v1/craig/events' | '/v1/craig/voice-packets', body: unknown): Promise<void> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    try {
      const response = await fetch(new URL(path, this.endpoint).toString(), {
        method: 'POST',
        headers: {
          authorization: `Bearer ${this.token}`,
          'content-type': 'application/json'
        },
        body: JSON.stringify(body),
        signal: controller.signal as any
      });
      if (!response.ok) throw new Error(`Meeting integration returned HTTP ${response.status}`);
    } finally {
      clearTimeout(timeout);
    }
  }
}

export async function createMeetingIntegrationSink(
  config: MeetingIntegrationConfig | undefined,
  logger: MeetingIntegrationLogger
): Promise<MeetingIntegrationSink> {
  if (!config?.enabled) return new NoopMeetingIntegrationSink();

  const endpoint = new URL(config.endpoint);
  if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password)
    throw new Error('Meeting integration endpoint must be an HTTP(S) URL without embedded credentials');

  const token = (await readFile(config.tokenFile, 'utf8')).trim();
  if (!token) throw new Error('Meeting integration token file is empty');

  const requestTimeoutMs = config.requestTimeoutMs ?? 5000;
  if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 100 || requestTimeoutMs > 60_000)
    throw new Error('Meeting integration requestTimeoutMs must be between 100 and 60000');

  logger.debug(`Meeting integration enabled for ${endpoint.origin}`);
  return new BoundedMeetingIntegrationSink(
    new HttpMeetingIntegrationTransport(endpoint, token, requestTimeoutMs),
    logger,
    config.maxQueuedPackets,
    config.batchSize
  );
}
