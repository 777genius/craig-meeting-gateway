import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { type Stats, createReadStream } from 'node:fs';
import { mkdir, mkdtemp, open, readdir, readFile, rename, rm, stat, unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import fetch from 'node-fetch';

import crc32 from './crc32';

const sourceFileKinds = ['data', 'header1', 'header2', 'users', 'info', 'log'] as const;
const maximumCookedTrackBytes = 64 * 1024 * 1024;
const authoritativeTimelineBasis = 'craig-cook-shared-origin-v1' as const;

type OriginalRecordingSourceFileKind = typeof sourceFileKinds[number];

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

export interface MeetingStartedLifecycleEvent extends MeetingLifecycleEvent {
  type: 'meeting.started';
  participantIds: string[];
}

export interface MeetingTerminalLifecycleEvent extends MeetingLifecycleEvent {
  type: 'meeting.ended' | 'meeting.aborted';
  reason: string | null;
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

export interface OriginalRecordingPublicationInput {
  startedEvent: MeetingStartedLifecycleEvent;
  terminalEvent: MeetingTerminalLifecycleEvent;
  sourceFileBase: string;
}

interface OriginalRecordingSourceFileReference {
  kind: OriginalRecordingSourceFileKind;
  relativePath: string;
  checksumSha256?: string;
  sizeBytes?: number;
}

interface PreparedAuthoritativeTrack {
  speakerId: string;
  trackNumber: number;
  timelineOffsetMs: number;
}

interface OriginalRecordingOutboxJob {
  schemaVersion: 2;
  publicationId: string;
  recordingId: string;
  guildId: string;
  channelId: string;
  startedEvent: MeetingStartedLifecycleEvent;
  terminalEvent: MeetingTerminalLifecycleEvent;
  sourceFiles: OriginalRecordingSourceFileReference[];
  authoritativeTracks?: PreparedAuthoritativeTrack[];
  authoritativeTimelineBasis?: typeof authoritativeTimelineBasis;
}

interface PendingOriginalRecordingJob {
  filePath: string;
  job: OriginalRecordingOutboxJob;
}

export interface AuthoritativeTrackMetadata {
  schemaVersion: 1;
  uploadId: string;
  recordingId: string;
  guildId: string;
  channelId: string;
  speakerId: string;
  trackNumber: number;
  timelineOffsetMs: number;
  checksumSha256: string;
  sizeBytes: number;
}

export interface AuthoritativeRecordingReadyEvent {
  schemaVersion: 1;
  eventId: string;
  recordingId: string;
  guildId: string;
  channelId: string;
  occurredAt: string;
  type: 'recording.authoritative_ready';
  endedAt: string;
  trackCount: number;
  sourceFilesChecksumSha256: string;
}

export interface CookedAuthoritativeTrack {
  filePath: string;
  checksumSha256: string;
  sizeBytes: number;
  dispose(): Promise<void>;
}

export interface OriginalRecordingCooker {
  cook(recordingId: string, trackNumber: number): Promise<CookedAuthoritativeTrack>;
}

export interface OriginalRecordingOutboxOptions {
  recordingRoot: string;
  outboxRoot?: string;
  cooker?: OriginalRecordingCooker;
}

export interface MeetingIntegrationLogger {
  debug(message: string): void;
  error(message: string, error?: unknown): void;
  warn(message: string): void;
}

export interface MeetingIntegrationTransport {
  post(path: '/v1/craig/events' | '/v1/craig/voice-packets', body: unknown): Promise<void>;
  postAuthoritativeTrack?(metadata: AuthoritativeTrackMetadata, audioFilePath: string): Promise<void>;
  postAuthoritativeReady?(event: AuthoritativeRecordingReadyEvent): Promise<void>;
}

export interface MeetingIntegrationSink {
  publishLifecycle(event: MeetingLifecycleEvent): boolean;
  publishPacket(packet: MeetingVoicePacket, opus: Buffer): boolean;
  publishOriginalRecording(input: OriginalRecordingPublicationInput): Promise<boolean>;
  restoreOriginalRecordingJobs(): Promise<void>;
  drain(timeoutMs: number): Promise<boolean>;
}

export class MeetingIntegrationDeliveryError extends Error {
  constructor(message: string, readonly retryable: boolean, readonly status?: number) {
    super(message);
    this.name = 'MeetingIntegrationDeliveryError';
  }
}

export class PermanentOriginalRecordingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentOriginalRecordingError';
  }
}

export function isRetryableMeetingIntegrationStatus(status: number): boolean {
  return status === 408 || status === 409 || status === 425 || status === 429 || status >= 500;
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

  async publishOriginalRecording(): Promise<boolean> {
    return true;
  }

  async restoreOriginalRecordingJobs(): Promise<void> {}

  async drain(): Promise<boolean> {
    return true;
  }
}

export class CraigOriginalRecordingCooker implements OriginalRecordingCooker {
  constructor(
    private readonly scriptPath = path.resolve(__dirname, '../../../../../cook/raw-partwise.sh'),
    private readonly maxTrackBytes = maximumCookedTrackBytes
  ) {
    if (!Number.isSafeInteger(maxTrackBytes) || maxTrackBytes < 1) throw new Error('maxTrackBytes must be a positive integer');
  }

  async cook(recordingId: string, trackNumber: number): Promise<CookedAuthoritativeTrack> {
    assertRecordingId(recordingId);
    if (!Number.isSafeInteger(trackNumber) || trackNumber < 1) throw new Error('trackNumber must be a positive integer');

    const temporaryRoot = await mkdtemp(path.join(tmpdir(), 'craig-authoritative-track-'));
    const filePath = path.join(temporaryRoot, `${recordingId}-${trackNumber}.ogg`);
    const output = await open(filePath, 'wx', 0o600);
    const child = spawn(this.scriptPath, [recordingId, String(trackNumber)], {
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let stderr = '';
    child.stderr.on('data', (chunk: Buffer | string) => {
      if (stderr.length < 4096) stderr += (typeof chunk === 'string' ? chunk : chunk.toString('utf8')).slice(0, 4096 - stderr.length);
    });
    const exited = new Promise<{
      code: number | null;
      signal: NodeJS.Signals | null;
    }>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (code, signal) => resolve({ code, signal }));
    });
    const checksum = createHash('sha256');
    let sizeBytes = 0;
    let leadingBytes = Buffer.alloc(0);

    try {
      for await (const rawChunk of child.stdout) {
        const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk as Uint8Array);
        sizeBytes += chunk.byteLength;
        if (sizeBytes > this.maxTrackBytes) {
          child.kill('SIGKILL');
          throw new PermanentOriginalRecordingError(`Cooked authoritative track exceeds ${this.maxTrackBytes} bytes`);
        }
        if (leadingBytes.length < 4) leadingBytes = Buffer.concat([leadingBytes, chunk.subarray(0, 4 - leadingBytes.length)]);
        checksum.update(chunk);
        await output.writeFile(chunk);
      }
      const { code, signal } = await exited;
      if (code === null || code === 124 || code === 125 || code === 137 || code === 143)
        throw new Error(`Craig authoritative track cook was interrupted (${code ?? signal ?? 'unknown'}): ${stderr.trim()}`);
      if (code !== 0)
        throw new PermanentOriginalRecordingError(`Craig authoritative track cook failed (${code ?? signal ?? 'unknown'}): ${stderr.trim()}`);
      if (sizeBytes === 0 || leadingBytes.toString('ascii') !== 'OggS')
        throw new PermanentOriginalRecordingError('Craig authoritative track cook did not produce an Ogg stream');
      await output.sync();
      await output.close();
      return {
        filePath,
        checksumSha256: checksum.digest('hex'),
        sizeBytes,
        dispose: async () => rm(temporaryRoot, { recursive: true, force: true })
      };
    } catch (error) {
      child.kill('SIGKILL');
      await exited.catch(() => undefined);
      await output.close().catch(() => undefined);
      await rm(temporaryRoot, { recursive: true, force: true });
      throw error;
    }
  }
}

export class BoundedMeetingIntegrationSink implements MeetingIntegrationSink {
  private readonly queue: QueueItem[] = [];
  private readonly originalJobs: PendingOriginalRecordingJob[] = [];
  private readonly drainWaiters = new Set<() => void>();
  private readonly openRecordings = new Set<string>();
  private processing = false;
  private retryTimer: NodeJS.Timeout | null = null;
  private processingOriginal = false;
  private originalRetryTimer: NodeJS.Timeout | null = null;
  private queuedPackets = 0;
  private consecutiveFailures = 0;
  private consecutiveOriginalFailures = 0;
  private readonly recordingRoot?: string;
  private readonly pendingOriginalRoot?: string;
  private readonly rejectedOriginalRoot?: string;
  private readonly originalCooker?: OriginalRecordingCooker;

  constructor(
    private readonly transport: MeetingIntegrationTransport,
    private readonly logger: MeetingIntegrationLogger,
    private readonly maxQueuedPackets = 8192,
    private readonly batchSize = 128,
    private readonly maxQueuedLifecycleEvents = 1024,
    originalRecording?: OriginalRecordingOutboxOptions
  ) {
    if (!Number.isSafeInteger(maxQueuedPackets) || maxQueuedPackets < 1) throw new Error('maxQueuedPackets must be a positive integer');
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > maxQueuedPackets)
      throw new Error('batchSize must be a positive integer no greater than maxQueuedPackets');
    if (!Number.isSafeInteger(maxQueuedLifecycleEvents) || maxQueuedLifecycleEvents < 2)
      throw new Error('maxQueuedLifecycleEvents must reserve room for start and terminal events');
    if (originalRecording !== undefined) {
      this.recordingRoot = path.resolve(originalRecording.recordingRoot);
      const outboxRoot = path.resolve(originalRecording.outboxRoot ?? path.join(this.recordingRoot, '.meeting-integration-outbox'));
      this.pendingOriginalRoot = path.join(outboxRoot, 'pending');
      this.rejectedOriginalRoot = path.join(outboxRoot, 'rejected');
      this.originalCooker = originalRecording.cooker ?? new CraigOriginalRecordingCooker();
    }
  }

  publishLifecycle(event: MeetingLifecycleEvent): boolean {
    // Lifecycle traffic is tiny and must remain ordered with the accepted audio.
    const queuedLifecycleEvents = this.queue.length - this.queuedPackets;
    const isTerminal = event.type === 'meeting.ended' || event.type === 'meeting.aborted';
    if (event.type === 'meeting.started') {
      if (this.openRecordings.has(event.recordingId)) return false;
      if (queuedLifecycleEvents + this.openRecordings.size + 2 > this.maxQueuedLifecycleEvents) return false;
      this.openRecordings.add(event.recordingId);
    } else if (!this.openRecordings.has(event.recordingId)) return false;
    else if (!isTerminal && queuedLifecycleEvents + this.openRecordings.size + 1 > this.maxQueuedLifecycleEvents) return false;

    this.queue.push({ type: 'lifecycle', event });
    if (isTerminal) this.openRecordings.delete(event.recordingId);
    this.scheduleProcessing();
    return true;
  }

  publishPacket(packet: MeetingVoicePacket, opus: Buffer): boolean {
    if (!this.openRecordings.has(packet.recordingId)) return false;
    if (this.queuedPackets >= this.maxQueuedPackets) return false;

    // Clone only after synchronous bounded admission, never before it.
    const opusBase64 = Buffer.from(opus).toString('base64');
    this.queue.push({ type: 'voice', packet: { ...packet, opusBase64 } });
    this.queuedPackets++;
    this.scheduleProcessing();
    return true;
  }

  async publishOriginalRecording(input: OriginalRecordingPublicationInput): Promise<boolean> {
    const recordingId = input.startedEvent.recordingId;
    if (this.recordingRoot === undefined || this.pendingOriginalRoot === undefined || this.rejectedOriginalRoot === undefined) {
      this.logger.warn(`Meeting original recording outbox is not configured for ${recordingId}`);
      return false;
    }

    try {
      const job = createOriginalRecordingJob(input, this.recordingRoot);
      await this.ensureOriginalOutboxDirectories();
      const filePath = path.join(this.pendingOriginalRoot, `${job.recordingId}.json`);
      const existing = await readOriginalRecordingJob(filePath).catch((error: NodeJS.ErrnoException) => {
        if (error.code === 'ENOENT') return undefined;
        throw error;
      });
      const queuedJob = existing ?? job;
      if (existing === undefined) await writeOriginalRecordingJob(filePath, job);
      else if (existing.publicationId !== job.publicationId)
        throw new Error(`Original recording outbox contains a conflicting job for ${job.recordingId}`);
      this.enqueueOriginalJob({ filePath, job: queuedJob });
      this.scheduleOriginalProcessing();
      return true;
    } catch (error) {
      this.logger.error(`Failed to persist original recording outbox job for ${recordingId}; original Craig files remain authoritative`, error);
      return false;
    }
  }

  async restoreOriginalRecordingJobs(): Promise<void> {
    if (this.pendingOriginalRoot === undefined || this.rejectedOriginalRoot === undefined) return;
    try {
      await this.ensureOriginalOutboxDirectories();
      const entries = (await readdir(this.pendingOriginalRoot)).filter((entry) => entry.endsWith('.json')).sort();
      for (const entry of entries) {
        const filePath = path.join(this.pendingOriginalRoot, entry);
        try {
          this.enqueueOriginalJob({
            filePath,
            job: await readOriginalRecordingJob(filePath)
          });
        } catch (error) {
          this.logger.error(`Rejecting invalid Meeting original recording outbox job ${entry}`, error);
          await this.rejectOriginalJob(filePath, entry);
        }
      }
      if (entries.length > 0) this.logger.debug(`Restored ${this.originalJobs.length} Meeting original recording outbox jobs`);
      this.scheduleOriginalProcessing();
    } catch (error) {
      this.logger.error('Failed to scan Meeting original recording outbox; original Craig files remain untouched', error);
    }
  }

  async drain(timeoutMs: number): Promise<boolean> {
    if (this.isDrained()) return true;
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
      this.scheduleOriginalProcessing();
    });
  }

  private async ensureOriginalOutboxDirectories(): Promise<void> {
    await Promise.all([
      mkdir(this.pendingOriginalRoot!, { recursive: true, mode: 0o700 }),
      mkdir(this.rejectedOriginalRoot!, { recursive: true, mode: 0o700 })
    ]);
  }

  private enqueueOriginalJob(pending: PendingOriginalRecordingJob): void {
    if (this.originalJobs.some(({ job }) => job.publicationId === pending.job.publicationId)) return;
    this.originalJobs.push(pending);
    this.originalJobs.sort(
      (left, right) =>
        left.job.terminalEvent.occurredAt.localeCompare(right.job.terminalEvent.occurredAt) ||
        left.job.publicationId.localeCompare(right.job.publicationId)
    );
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
        await this.transport.post('/v1/craig/voice-packets', {
          schemaVersion: 1,
          packets: batch
        });
        this.queue.splice(0, batch.length);
        this.queuedPackets -= batch.length;
      }
      this.consecutiveFailures = 0;
    } catch (error) {
      if (isRetryableDeliveryError(error)) {
        this.consecutiveFailures++;
        const delayMs = retryDelay(this.consecutiveFailures);
        this.logger.error(`Meeting integration delivery failed; retrying in ${delayMs}ms`, error);
        this.processing = false;
        this.scheduleProcessing(delayMs);
        return;
      }

      const first = this.queue[0];
      if (first.type === 'lifecycle') this.queue.shift();
      else {
        let discardedPackets = 0;
        for (const item of this.queue) {
          if (item.type !== 'voice' || discardedPackets >= this.batchSize) break;
          discardedPackets++;
        }
        this.queue.splice(0, discardedPackets);
        this.queuedPackets -= discardedPackets;
      }
      this.consecutiveFailures = 0;
      this.logger.error('Meeting integration delivery was permanently rejected; discarding it so FIFO can continue', error);
    }

    this.processing = false;
    if (this.queue.length > 0) this.scheduleProcessing();
    else this.scheduleOriginalProcessing();
    this.notifyIfDrained();
  }

  private scheduleOriginalProcessing(delayMs = 0): void {
    if (
      this.processingOriginal ||
      this.originalRetryTimer ||
      this.originalJobs.length === 0 ||
      this.processing ||
      this.queue.length > 0 ||
      this.originalCooker === undefined ||
      this.transport.postAuthoritativeTrack === undefined ||
      this.transport.postAuthoritativeReady === undefined
    )
      return;
    this.originalRetryTimer = setTimeout(() => {
      this.originalRetryTimer = null;
      if (this.processing || this.queue.length > 0) return;
      void this.processOriginal();
    }, delayMs);
  }

  private async processOriginal(): Promise<void> {
    if (this.processingOriginal || this.originalJobs.length === 0 || this.processing || this.queue.length > 0) return;
    this.processingOriginal = true;
    const pending = this.originalJobs[0];

    try {
      pending.job = await this.prepareOriginalJob(pending);
      await this.deliverOriginalJob(pending.job);
      await unlink(pending.filePath);
      this.originalJobs.shift();
      this.consecutiveOriginalFailures = 0;
    } catch (error) {
      if (isRetryableDeliveryError(error)) {
        this.consecutiveOriginalFailures++;
        const delayMs = retryDelay(this.consecutiveOriginalFailures);
        this.logger.error(`Meeting original recording publication failed; retrying in ${delayMs}ms`, error);
        this.processingOriginal = false;
        this.scheduleOriginalProcessing(delayMs);
        return;
      }

      this.logger.error(
        `Meeting original recording publication ${pending.job.publicationId} was permanently rejected; retaining it in rejected outbox`,
        error
      );
      try {
        await this.rejectOriginalJob(pending.filePath, path.basename(pending.filePath));
      } catch (rejectionError) {
        this.logger.error(
          `Failed to retain rejected original recording job ${pending.job.publicationId}; retrying outbox transition`,
          rejectionError
        );
        this.processingOriginal = false;
        this.scheduleOriginalProcessing(retryDelay(1));
        return;
      }
      this.originalJobs.shift();
      this.consecutiveOriginalFailures = 0;
    }

    this.processingOriginal = false;
    if (this.originalJobs.length > 0) this.scheduleOriginalProcessing();
    this.notifyIfDrained();
  }

  private async prepareOriginalJob(pending: PendingOriginalRecordingJob): Promise<OriginalRecordingOutboxJob> {
    if (pending.job.terminalEvent.type === 'meeting.aborted') return pending.job;
    if (
      pending.job.authoritativeTimelineBasis === authoritativeTimelineBasis &&
      pending.job.authoritativeTracks !== undefined &&
      pending.job.sourceFiles.every((source) => source.checksumSha256 !== undefined && source.sizeBytes !== undefined)
    )
      return pending.job;

    const usersSource = pending.job.sourceFiles.find(({ kind }) => kind === 'users')!;
    const dataSource = pending.job.sourceFiles.find(({ kind }) => kind === 'data')!;
    const users = await inspectOriginalRecordingUsers(this.resolveSourceFile(usersSource));
    const data = await inspectOriginalRecordingData(
      this.resolveSourceFile(dataSource),
      users.tracks.map(({ trackNumber }) => trackNumber)
    );
    const sourceFiles: OriginalRecordingSourceFileReference[] = [];
    for (const source of pending.job.sourceFiles) {
      const digest =
        source.kind === 'users' ? users.integrity : source.kind === 'data' ? data.integrity : await digestFile(this.resolveSourceFile(source));
      assertOriginalSourceIntegrity(source, digest);
      sourceFiles.push({ ...source, ...digest });
    }
    const prepared: OriginalRecordingOutboxJob = {
      ...pending.job,
      sourceFiles,
      authoritativeTracks: users.tracks.map((track) => ({
        ...track,
        timelineOffsetMs: data.timelineOffsetMs
      })),
      authoritativeTimelineBasis
    };
    await writeOriginalRecordingJob(pending.filePath, prepared);
    return prepared;
  }

  private async deliverOriginalJob(job: OriginalRecordingOutboxJob): Promise<void> {
    await this.transport.post('/v1/craig/events', job.startedEvent);
    await this.transport.post('/v1/craig/events', job.terminalEvent);
    if (job.terminalEvent.type === 'meeting.aborted') return;

    if (job.authoritativeTracks === undefined) throw new PermanentOriginalRecordingError('Original recording track metadata was not prepared');
    for (const track of job.authoritativeTracks) {
      const cooked = await this.originalCooker!.cook(job.recordingId, track.trackNumber);
      try {
        await this.transport.postAuthoritativeTrack!(
          {
            schemaVersion: 1,
            uploadId: `authoritative-track:v1:${job.recordingId}:${track.trackNumber}`,
            recordingId: job.recordingId,
            guildId: job.guildId,
            channelId: job.channelId,
            speakerId: track.speakerId,
            trackNumber: track.trackNumber,
            timelineOffsetMs: track.timelineOffsetMs,
            checksumSha256: cooked.checksumSha256,
            sizeBytes: cooked.sizeBytes
          },
          cooked.filePath
        );
      } finally {
        await cooked.dispose().catch((error) => this.logger.warn(`Failed to remove cooked track ${cooked.filePath}: ${String(error)}`));
      }
    }

    await this.transport.postAuthoritativeReady!({
      schemaVersion: 1,
      eventId: `${job.recordingId}:authoritative-ready:v1`,
      recordingId: job.recordingId,
      guildId: job.guildId,
      channelId: job.channelId,
      occurredAt: job.terminalEvent.occurredAt,
      type: 'recording.authoritative_ready',
      endedAt: job.terminalEvent.occurredAt,
      trackCount: job.authoritativeTracks.length,
      sourceFilesChecksumSha256: sourceFilesChecksum(job.sourceFiles)
    });
  }

  private resolveSourceFile(source: OriginalRecordingSourceFileReference): string {
    const resolved = path.resolve(this.recordingRoot!, source.relativePath);
    if (path.dirname(resolved) !== this.recordingRoot)
      throw new Error(`Original recording source path escaped recording root: ${source.relativePath}`);
    return resolved;
  }

  private async rejectOriginalJob(filePath: string, fileName: string): Promise<void> {
    const destination = path.join(this.rejectedOriginalRoot!, fileName);
    await rename(filePath, destination).catch(async (error: NodeJS.ErrnoException) => {
      if (error.code !== 'ENOENT') throw error;
    });
  }

  private isDrained(): boolean {
    return this.queue.length === 0 && !this.processing && this.originalJobs.length === 0 && !this.processingOriginal;
  }

  private notifyIfDrained(): void {
    if (!this.isDrained()) return;
    for (const waiter of this.drainWaiters) waiter();
    this.drainWaiters.clear();
  }
}

export class HttpMeetingIntegrationTransport implements MeetingIntegrationTransport {
  constructor(private readonly endpoint: URL, private readonly token: string, private readonly requestTimeoutMs: number) {}

  async post(path: '/v1/craig/events' | '/v1/craig/voice-packets', body: unknown): Promise<void> {
    await this.request(path, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${this.token}`,
        'content-type': 'application/json'
      },
      body: JSON.stringify(body)
    });
  }

  async postAuthoritativeTrack(metadata: AuthoritativeTrackMetadata, audioFilePath: string): Promise<void> {
    const stream = createReadStream(audioFilePath);
    try {
      await this.request('/v1/craig/authoritative-tracks', {
        method: 'POST',
        headers: {
          authorization: `Bearer ${this.token}`,
          'content-length': String(metadata.sizeBytes),
          'content-type': 'audio/ogg',
          'x-craig-authoritative-track-metadata': Buffer.from(JSON.stringify(metadata), 'utf8').toString('base64url')
        },
        body: stream as any
      });
    } finally {
      stream.destroy();
    }
  }

  async postAuthoritativeReady(event: AuthoritativeRecordingReadyEvent): Promise<void> {
    await this.request(
      '/v1/craig/events',
      {
        method: 'POST',
        headers: {
          authorization: `Bearer ${this.token}`,
          'content-type': 'application/json'
        },
        body: JSON.stringify(event)
      },
      202
    );
  }

  private async request(pathname: string, init: Parameters<typeof fetch>[1], expectedStatus?: number): Promise<void> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    try {
      const response = await fetch(new URL(pathname, this.endpoint).toString(), { ...init, signal: controller.signal as any });
      if ((expectedStatus === undefined && !response.ok) || (expectedStatus !== undefined && response.status !== expectedStatus))
        throw new MeetingIntegrationDeliveryError(
          `Meeting integration returned HTTP ${response.status}`,
          isRetryableMeetingIntegrationStatus(response.status),
          response.status
        );
    } finally {
      clearTimeout(timeout);
    }
  }
}

export async function createMeetingIntegrationSink(
  config: MeetingIntegrationConfig | undefined,
  logger: MeetingIntegrationLogger,
  recordingRoot?: string
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
  const sink = new BoundedMeetingIntegrationSink(
    new HttpMeetingIntegrationTransport(endpoint, token, requestTimeoutMs),
    logger,
    config.maxQueuedPackets,
    config.batchSize,
    1024,
    recordingRoot === undefined ? undefined : { recordingRoot }
  );
  await sink.restoreOriginalRecordingJobs();
  return sink;
}

function retryDelay(consecutiveFailures: number): number {
  return Math.min(10_000, 100 * 2 ** Math.min(consecutiveFailures - 1, 7));
}

function isRetryableDeliveryError(error: unknown): boolean {
  if (error instanceof PermanentOriginalRecordingError) return false;
  return !(error instanceof MeetingIntegrationDeliveryError) || error.retryable;
}

function assertRecordingId(recordingId: string): void {
  if (!/^[0-9A-Za-z_-]{1,128}$/.test(recordingId)) throw new Error('recordingId contains unsafe characters');
}

function parseLifecycleEnvelope(value: unknown, expectedFields: readonly string[]): Record<string, unknown> {
  if (!isRecord(value) || Object.keys(value).length !== expectedFields.length || expectedFields.some((field) => !(field in value)))
    throw new Error('Original recording outbox lifecycle event is malformed');
  if (
    value.schemaVersion !== 1 ||
    typeof value.eventId !== 'string' ||
    value.eventId.length < 1 ||
    value.eventId.length > 128 ||
    typeof value.recordingId !== 'string' ||
    typeof value.guildId !== 'string' ||
    typeof value.channelId !== 'string' ||
    typeof value.occurredAt !== 'string'
  )
    throw new Error('Original recording outbox lifecycle envelope is invalid');
  assertRecordingId(value.recordingId);
  if (!/^\d{16,22}$/.test(value.guildId) || !/^\d{16,22}$/.test(value.channelId))
    throw new Error('Original recording outbox lifecycle identity is invalid');
  if (!isCanonicalInstant(value.occurredAt)) throw new Error('Original recording outbox lifecycle timestamp is invalid');
  return value;
}

function parseStartedLifecycleEvent(value: unknown): MeetingStartedLifecycleEvent {
  const parsed = parseLifecycleEnvelope(value, [
    'schemaVersion',
    'eventId',
    'recordingId',
    'guildId',
    'channelId',
    'occurredAt',
    'type',
    'participantIds'
  ]);
  if (
    parsed.type !== 'meeting.started' ||
    !Array.isArray(parsed.participantIds) ||
    parsed.participantIds.length > 1000 ||
    parsed.participantIds.some((participantId) => typeof participantId !== 'string' || !/^\d{16,22}$/.test(participantId))
  )
    throw new Error('Original recording outbox meeting.started event is invalid');
  return {
    schemaVersion: 1,
    eventId: parsed.eventId as string,
    recordingId: parsed.recordingId as string,
    guildId: parsed.guildId as string,
    channelId: parsed.channelId as string,
    occurredAt: parsed.occurredAt as string,
    type: 'meeting.started',
    participantIds: [...parsed.participantIds] as string[]
  };
}

function parseTerminalLifecycleEvent(value: unknown): MeetingTerminalLifecycleEvent {
  const parsed = parseLifecycleEnvelope(value, ['schemaVersion', 'eventId', 'recordingId', 'guildId', 'channelId', 'occurredAt', 'type', 'reason']);
  if (
    (parsed.type !== 'meeting.ended' && parsed.type !== 'meeting.aborted') ||
    (parsed.reason !== null && (typeof parsed.reason !== 'string' || parsed.reason.length < 1 || parsed.reason.length > 256))
  )
    throw new Error('Original recording outbox terminal lifecycle event is invalid');
  return {
    schemaVersion: 1,
    eventId: parsed.eventId as string,
    recordingId: parsed.recordingId as string,
    guildId: parsed.guildId as string,
    channelId: parsed.channelId as string,
    occurredAt: parsed.occurredAt as string,
    type: parsed.type,
    reason: parsed.reason as string | null
  };
}

function assertMatchingLifecycleIdentity(startedEvent: MeetingStartedLifecycleEvent, terminalEvent: MeetingTerminalLifecycleEvent): void {
  if (
    terminalEvent.recordingId !== startedEvent.recordingId ||
    terminalEvent.guildId !== startedEvent.guildId ||
    terminalEvent.channelId !== startedEvent.channelId ||
    terminalEvent.eventId === startedEvent.eventId ||
    Date.parse(terminalEvent.occurredAt) < Date.parse(startedEvent.occurredAt)
  )
    throw new Error('Original recording outbox lifecycle events do not describe one ordered recording');
}

function isCanonicalInstant(value: string): boolean {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}

function createOriginalRecordingJob(input: OriginalRecordingPublicationInput, recordingRoot: string): OriginalRecordingOutboxJob {
  const startedEvent = parseStartedLifecycleEvent(input.startedEvent);
  const terminalEvent = parseTerminalLifecycleEvent(input.terminalEvent);
  assertMatchingLifecycleIdentity(startedEvent, terminalEvent);
  const { recordingId, guildId, channelId } = startedEvent;
  assertRecordingId(recordingId);

  const resolvedRoot = path.resolve(recordingRoot);
  const resolvedBase = path.resolve(input.sourceFileBase);
  if (path.dirname(resolvedBase) !== resolvedRoot || path.basename(resolvedBase) !== `${recordingId}.ogg`)
    throw new Error('sourceFileBase must identify the recording inside recordingRoot');

  return {
    schemaVersion: 2,
    publicationId: `authoritative-recording:v1:${recordingId}`,
    recordingId,
    guildId,
    channelId,
    startedEvent,
    terminalEvent,
    sourceFiles: sourceFileKinds.map((kind) => ({
      kind,
      relativePath: `${recordingId}.ogg.${kind}`
    }))
  };
}

async function writeOriginalRecordingJob(filePath: string, job: OriginalRecordingOutboxJob): Promise<void> {
  const temporaryPath = path.join(path.dirname(filePath), `.${path.basename(filePath)}.${process.pid}.${Date.now()}.tmp`);
  const descriptor = await open(temporaryPath, 'wx', 0o600);
  try {
    await descriptor.writeFile(`${JSON.stringify(job)}\n`, 'utf8');
    await descriptor.sync();
  } finally {
    await descriptor.close();
  }
  try {
    await rename(temporaryPath, filePath);
    await syncDirectory(path.dirname(filePath));
  } catch (error) {
    await unlink(temporaryPath).catch(() => undefined);
    throw error;
  }
}

async function syncDirectory(directoryPath: string): Promise<void> {
  const descriptor = await open(directoryPath, 'r');
  try {
    await descriptor.sync();
  } finally {
    await descriptor.close();
  }
}

async function readOriginalRecordingJob(filePath: string): Promise<OriginalRecordingOutboxJob> {
  const parsed = JSON.parse(await readFile(filePath, 'utf8')) as unknown;
  if (!isRecord(parsed) || parsed.schemaVersion !== 2) throw new Error('Original recording outbox job has an unsupported schema');
  const {
    publicationId,
    recordingId,
    guildId,
    channelId,
    startedEvent: rawStartedEvent,
    terminalEvent: rawTerminalEvent,
    sourceFiles,
    authoritativeTracks: rawAuthoritativeTracks,
    authoritativeTimelineBasis: rawAuthoritativeTimelineBasis
  } = parsed;
  if (
    typeof publicationId !== 'string' ||
    typeof recordingId !== 'string' ||
    typeof guildId !== 'string' ||
    typeof channelId !== 'string' ||
    !Array.isArray(sourceFiles)
  )
    throw new Error('Original recording outbox job is malformed');
  assertRecordingId(recordingId);
  if (publicationId !== `authoritative-recording:v1:${recordingId}` || !/^\d{16,22}$/.test(guildId) || !/^\d{16,22}$/.test(channelId))
    throw new Error('Original recording outbox job identity is invalid');
  const startedEvent = parseStartedLifecycleEvent(rawStartedEvent);
  const terminalEvent = parseTerminalLifecycleEvent(rawTerminalEvent);
  assertMatchingLifecycleIdentity(startedEvent, terminalEvent);
  if (startedEvent.recordingId !== recordingId || startedEvent.guildId !== guildId || startedEvent.channelId !== channelId)
    throw new Error('Original recording outbox lifecycle identity does not match its job');
  if (sourceFiles.length !== sourceFileKinds.length) throw new Error('Original recording outbox source file set is incomplete');

  const normalizedSources = sourceFiles.map((source): OriginalRecordingSourceFileReference => {
    if (!isRecord(source) || typeof source.kind !== 'string' || typeof source.relativePath !== 'string')
      throw new Error('Original recording outbox source file is malformed');
    if (!sourceFileKinds.includes(source.kind as OriginalRecordingSourceFileKind))
      throw new Error(`Unknown original recording source kind ${source.kind}`);
    if (source.relativePath !== `${recordingId}.ogg.${source.kind}`) throw new Error('Original recording outbox source path is invalid');
    const checksumSha256 = source.checksumSha256;
    const sizeBytes = source.sizeBytes;
    if ((checksumSha256 === undefined) !== (sizeBytes === undefined)) throw new Error('Original recording source integrity metadata is incomplete');
    if (checksumSha256 !== undefined && (typeof checksumSha256 !== 'string' || !/^[0-9a-f]{64}$/.test(checksumSha256)))
      throw new Error('Original recording source checksum is invalid');
    if (sizeBytes !== undefined && (typeof sizeBytes !== 'number' || !Number.isSafeInteger(sizeBytes) || sizeBytes < 0))
      throw new Error('Original recording source size is invalid');
    return {
      kind: source.kind as OriginalRecordingSourceFileKind,
      relativePath: source.relativePath,
      ...(checksumSha256 === undefined ? {} : { checksumSha256, sizeBytes: sizeBytes as number })
    };
  });
  if (new Set(normalizedSources.map(({ kind }) => kind)).size !== sourceFileKinds.length)
    throw new Error('Original recording outbox source kinds must be unique');

  const authoritativeTracks = parsePreparedAuthoritativeTracks(rawAuthoritativeTracks);
  if (rawAuthoritativeTimelineBasis !== undefined && rawAuthoritativeTimelineBasis !== authoritativeTimelineBasis)
    throw new Error('Original recording outbox timeline basis is invalid');
  if (
    rawAuthoritativeTimelineBasis === authoritativeTimelineBasis &&
    (authoritativeTracks === undefined || new Set(authoritativeTracks.map(({ timelineOffsetMs }) => timelineOffsetMs)).size !== 1)
  )
    throw new Error('Original recording outbox shared timeline metadata is invalid');

  return {
    schemaVersion: 2,
    publicationId,
    recordingId,
    guildId,
    channelId,
    startedEvent,
    terminalEvent,
    sourceFiles: normalizedSources.sort((left, right) => sourceFileKinds.indexOf(left.kind) - sourceFileKinds.indexOf(right.kind)),
    ...(authoritativeTracks === undefined ? {} : { authoritativeTracks }),
    ...(rawAuthoritativeTimelineBasis === undefined ? {} : { authoritativeTimelineBasis })
  };
}

function parsePreparedAuthoritativeTracks(value: unknown): PreparedAuthoritativeTrack[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length < 1 || value.length > 64) throw new Error('Original recording outbox track metadata is malformed');
  const tracks = value.map((track): PreparedAuthoritativeTrack => {
    if (
      !isRecord(track) ||
      typeof track.speakerId !== 'string' ||
      !/^\d{17,20}$/.test(track.speakerId) ||
      typeof track.trackNumber !== 'number' ||
      !Number.isSafeInteger(track.trackNumber) ||
      track.trackNumber < 1 ||
      track.trackNumber > 1000 ||
      typeof track.timelineOffsetMs !== 'number' ||
      !Number.isSafeInteger(track.timelineOffsetMs) ||
      track.timelineOffsetMs < 0
    )
      throw new Error('Original recording outbox track metadata is invalid');
    return {
      speakerId: track.speakerId,
      trackNumber: track.trackNumber,
      timelineOffsetMs: track.timelineOffsetMs
    };
  });
  if (new Set(tracks.map(({ trackNumber }) => trackNumber)).size !== tracks.length)
    throw new Error('Original recording outbox track numbers must be unique');
  if (new Set(tracks.map(({ speakerId }) => speakerId)).size !== tracks.length)
    throw new Error('Original recording outbox speaker identities must be unique');
  return tracks.sort((left, right) => left.trackNumber - right.trackNumber);
}

async function digestFile(filePath: string): Promise<{ checksumSha256: string; sizeBytes: number }> {
  try {
    const descriptor = await stat(filePath);
    if (!descriptor.isFile()) throw new PermanentOriginalRecordingError(`Original recording source is not a regular file: ${filePath}`);
    const checksum = createHash('sha256');
    let sizeBytes = 0;
    for await (const rawChunk of createReadStream(filePath)) {
      const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk as Uint8Array);
      sizeBytes += chunk.byteLength;
      checksum.update(chunk);
    }
    if (sizeBytes !== descriptor.size) throw new PermanentOriginalRecordingError(`Original recording source changed while hashing: ${filePath}`);
    return { checksumSha256: checksum.digest('hex'), sizeBytes };
  } catch (error) {
    if (isMissingOriginalSourceError(error)) throw new PermanentOriginalRecordingError(`Original recording source is missing: ${filePath}`);
    throw error;
  }
}

async function inspectOriginalRecordingUsers(filePath: string): Promise<{
  tracks: Array<{ speakerId: string; trackNumber: number }>;
  integrity: { checksumSha256: string; sizeBytes: number };
}> {
  let contents: Buffer;
  try {
    const descriptor = await stat(filePath);
    if (!descriptor.isFile()) throw new PermanentOriginalRecordingError(`Craig users source is not a regular file: ${filePath}`);
    contents = await readFile(filePath);
    if (contents.byteLength !== descriptor.size) throw new PermanentOriginalRecordingError(`Craig users source changed while reading: ${filePath}`);
  } catch (error) {
    if (isMissingOriginalSourceError(error)) throw new PermanentOriginalRecordingError(`Craig users source is missing: ${filePath}`);
    throw error;
  }
  let users: unknown;
  try {
    users = JSON.parse(`{${contents.toString('utf8')}}`) as unknown;
  } catch {
    throw new PermanentOriginalRecordingError('Craig users source is not valid JSON');
  }
  if (!isRecord(users)) throw new PermanentOriginalRecordingError('Craig users source is malformed');
  const mapped: Array<{ speakerId: string; trackNumber: number }> = [];
  for (const [trackText, rawUser] of Object.entries(users)) {
    const trackNumber = Number(trackText);
    if (trackNumber === 0 && isRecord(rawUser) && Object.keys(rawUser).length === 0) continue;
    if (
      !Number.isSafeInteger(trackNumber) ||
      trackNumber < 1 ||
      trackNumber > 1000 ||
      !isRecord(rawUser) ||
      typeof rawUser.id !== 'string' ||
      !/^\d{17,20}$/.test(rawUser.id)
    )
      throw new PermanentOriginalRecordingError(`Craig users source contains an invalid track ${trackText}`);
    mapped.push({ speakerId: rawUser.id, trackNumber });
  }
  mapped.sort((left, right) => left.trackNumber - right.trackNumber);
  if (mapped.length < 1 || mapped.length > 64)
    throw new PermanentOriginalRecordingError('Craig users source must contain between 1 and 64 speaker tracks');
  if (new Set(mapped.map(({ trackNumber }) => trackNumber)).size !== mapped.length)
    throw new PermanentOriginalRecordingError('Craig users source contains duplicate track numbers');
  if (new Set(mapped.map(({ speakerId }) => speakerId)).size !== mapped.length)
    throw new PermanentOriginalRecordingError('Craig users source contains duplicate speaker identities');
  return {
    tracks: mapped,
    integrity: {
      checksumSha256: createHash('sha256').update(contents).digest('hex'),
      sizeBytes: contents.byteLength
    }
  };
}

async function inspectOriginalRecordingData(
  filePath: string,
  trackNumbers: number[]
): Promise<{
  timelineOffsetMs: number;
  integrity: { checksumSha256: string; sizeBytes: number };
}> {
  let descriptor: Stats;
  try {
    descriptor = await stat(filePath);
    if (!descriptor.isFile()) throw new PermanentOriginalRecordingError(`Craig data source is not a regular file: ${filePath}`);
  } catch (error) {
    if (isMissingOriginalSourceError(error)) throw new PermanentOriginalRecordingError(`Craig data source is missing: ${filePath}`);
    throw error;
  }

  const wantedTracks = new Set(trackNumbers);
  const tracksWithAudio = new Set<number>();
  let timelineOffsetMs: number | undefined;
  const checksum = createHash('sha256');
  let buffered = Buffer.alloc(0);
  let sizeBytes = 0;
  let pageNumber = 0;

  try {
    for await (const rawChunk of createReadStream(filePath)) {
      const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk as Uint8Array);
      checksum.update(chunk);
      sizeBytes += chunk.byteLength;
      buffered = buffered.byteLength === 0 ? chunk : Buffer.concat([buffered, chunk]);

      while (buffered.byteLength >= 4) {
        if (buffered.subarray(0, 4).toString('ascii') !== 'OggS')
          throw new PermanentOriginalRecordingError(`Craig data source has an invalid Ogg capture pattern at page ${pageNumber + 1}`);
        if (buffered.byteLength < 27) break;
        if (buffered[4] !== 0)
          throw new PermanentOriginalRecordingError(`Craig data source has an unsupported Ogg version at page ${pageNumber + 1}`);
        if ((buffered[5] & ~0x07) !== 0)
          throw new PermanentOriginalRecordingError(`Craig data source has invalid Ogg flags at page ${pageNumber + 1}`);

        const segmentCount = buffered[26];
        if (segmentCount === 0)
          throw new PermanentOriginalRecordingError(`Craig data source has an empty Ogg segment table at page ${pageNumber + 1}`);
        const headerBytes = 27 + segmentCount;
        if (buffered.byteLength < headerBytes) break;
        let packetBytes = 0;
        for (let index = 27; index < headerBytes; index++) packetBytes += buffered[index];
        if (buffered[headerBytes - 1] === 255)
          throw new PermanentOriginalRecordingError(`Craig data source has a continued Ogg packet at page ${pageNumber + 1}`);
        const pageBytes = headerBytes + packetBytes;
        if (buffered.byteLength < pageBytes) break;

        const page = buffered.subarray(0, pageBytes);
        const recordedChecksum = page.readInt32LE(22);
        const checksumInput = Buffer.from(page);
        checksumInput.fill(0, 22, 26);
        if (crc32(checksumInput) !== recordedChecksum)
          throw new PermanentOriginalRecordingError(`Craig data source has an invalid Ogg checksum at page ${pageNumber + 1}`);

        if (packetBytes > 0) {
          const trackNumber = page.readUInt32LE(14);
          const granule = page.readBigUInt64LE(6);
          if (granule === 0xffffffffffffffffn)
            throw new PermanentOriginalRecordingError(`Craig data source track ${trackNumber} has no audio granule position`);
          const pageOffsetMs = granule / 48n;
          if (pageOffsetMs > BigInt(Number.MAX_SAFE_INTEGER))
            throw new PermanentOriginalRecordingError(`Craig data source track ${trackNumber} has an unsafe audio granule position`);
          timelineOffsetMs ??= Number(pageOffsetMs);
          if (wantedTracks.has(trackNumber)) tracksWithAudio.add(trackNumber);
        }

        pageNumber++;
        buffered = buffered.subarray(pageBytes);
      }
    }
  } catch (error) {
    if (isMissingOriginalSourceError(error)) throw new PermanentOriginalRecordingError(`Craig data source is missing: ${filePath}`);
    throw error;
  }

  if (buffered.byteLength !== 0) throw new PermanentOriginalRecordingError(`Craig data source ends with a truncated Ogg page ${pageNumber + 1}`);
  if (sizeBytes !== descriptor.size) throw new PermanentOriginalRecordingError(`Craig data source changed while reading: ${filePath}`);
  for (const trackNumber of trackNumbers) {
    if (!tracksWithAudio.has(trackNumber)) throw new PermanentOriginalRecordingError(`Craig data source has no audio page for track ${trackNumber}`);
  }
  if (timelineOffsetMs === undefined) throw new PermanentOriginalRecordingError('Craig data source has no non-empty Ogg pages');
  return {
    timelineOffsetMs,
    integrity: { checksumSha256: checksum.digest('hex'), sizeBytes }
  };
}

function assertOriginalSourceIntegrity(source: OriginalRecordingSourceFileReference, actual: { checksumSha256: string; sizeBytes: number }): void {
  if (source.checksumSha256 === undefined && source.sizeBytes === undefined) return;
  if (source.checksumSha256 !== actual.checksumSha256 || source.sizeBytes !== actual.sizeBytes)
    throw new PermanentOriginalRecordingError(`Original recording source changed after outbox preparation: ${source.relativePath}`);
}

function isMissingOriginalSourceError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'ENOTDIR');
}

function sourceFilesChecksum(sourceFiles: OriginalRecordingSourceFileReference[]): string {
  if (sourceFiles.some(({ checksumSha256, sizeBytes }) => checksumSha256 === undefined || sizeBytes === undefined))
    throw new Error('Original recording source integrity metadata was not prepared');
  const canonical = sourceFiles.map(({ kind, relativePath, checksumSha256, sizeBytes }) => ({
    kind,
    relativePath,
    checksumSha256,
    sizeBytes
  }));
  return createHash('sha256').update(JSON.stringify(canonical), 'utf8').digest('hex');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
