import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  type Stats,
  closeSync,
  createReadStream,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmdirSync,
  unlinkSync,
  writeFileSync
} from 'node:fs';
import { mkdir, mkdtemp, open, readdir, readFile, rename, rm, stat, unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import fetch, { type Response } from 'node-fetch';

import crc32 from './crc32';
import {
  type CraigAuthoritativeCancellationPcmFenceLog,
  createAuthoritativeCancellationPcmFenceLog
} from './meetingCancellationPcmFenceV10';
import {
  type CraigLifecycleV3Event,
  type CraigLifecycleV3Admission,
  type DurableCraigLifecycleV3Snapshot,
  type MeetingLifecycleProducerConfiguration,
  parseMeetingLifecycleProducerConfiguration,
  restoreCraigLifecycleV3ProducerFromSnapshot
} from './meetingLifecycleV3';

const sourceFileKinds = ['data', 'header1', 'header2', 'users', 'info', 'log'] as const;
const maximumCookedTrackBytes = 64 * 1024 * 1024;
const authoritativeTimelineBasis = 'craig-cook-shared-origin-v1' as const;
const maximumMeetingPlatformConfigurationChannels = 64;
const discordSnowflake = /^\d{17,20}$/;

type OriginalRecordingSourceFileKind = typeof sourceFileKinds[number];

export interface MeetingIntegrationConfig {
  enabled: boolean;
  endpoint: string;
  tokenFile: string;
  maxQueuedPackets?: number;
  batchSize?: number;
  requestTimeoutMs?: number;
  /** Explicit capability rollout. Omit for schema-v1 compatible behavior. */
  lifecycleProducer?: unknown;
}

export interface MeetingPlatformConfigurationChannel {
  guildId: string;
  voiceChannelId: string;
}

export interface MeetingPlatformConfiguration {
  schemaVersion: 1;
  channels: readonly MeetingPlatformConfigurationChannel[];
}

export interface MeetingPlatformConfigurationClient {
  getConfiguration(): Promise<MeetingPlatformConfiguration>;
}

export interface LegacyMeetingLifecycleEvent {
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

export interface MeetingStartedLifecycleEvent extends LegacyMeetingLifecycleEvent {
  type: 'meeting.started';
  participantIds: string[];
}

export interface MeetingTerminalLifecycleEvent extends LegacyMeetingLifecycleEvent {
  type: 'meeting.ended' | 'meeting.aborted';
  reason: string | null;
}

export type MeetingLifecycleEvent = LegacyMeetingLifecycleEvent | CraigLifecycleV3Event;
export type AnyMeetingStartedLifecycleEvent = MeetingStartedLifecycleEvent | Extract<CraigLifecycleV3Event, { type: 'meeting.started' }>;
export type AnyMeetingTerminalLifecycleEvent =
  | MeetingTerminalLifecycleEvent
  | Extract<CraigLifecycleV3Event, { type: 'meeting.ended' | 'meeting.aborted' }>;

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

type LifecycleV3JournalState = {
  snapshot: DurableCraigLifecycleV3Snapshot;
  actorIndex: Map<string, DurableCraigLifecycleV3Snapshot['actors'][number]>;
  /** Sequence-indexed queue: ACK physically removes all per-event state in O(1). */
  pendingEvents: Map<number, CraigLifecycleV3Event>;
  eventDigests: Map<string, string>;
  generation: number;
  nextSequence: number;
  ackedSequence: number;
  closed: boolean;
  lastOccurredAt: string;
  lastAcknowledgedEventId: string | null;
  lastAcknowledgedDigest: string | null;
  maintenanceNeeded: boolean;
};

const lifecycleV3MaintenanceRecordsPerStep = 8;
const cancellationProofSelectionBudget = 8;
type LifecycleV3MaintenanceState = {
  schemaVersion: 1;
  phase: 'capture-base' | 'capture-deltas' | 'publish' | 'cleanup-deltas' | 'cleanup-generations';
  sourceGeneration: number;
  targetGeneration: number;
  targetSequence: number;
  baseActorCursor: number;
  deltaCursor: number;
  deltaActorCursor: number;
  chunkCount: number;
  chunksChecksumSha256: string;
  cleanupCursor: number;
  previousCoveredDeltaCursor: number;
  obsoleteGeneration: number | null;
  obsoleteChunkCount: number;
};

type LifecycleV3GenerationReference = Readonly<{
  generation: number;
  file: string;
  checksumSha256: string;
  coveredDeltaCursor: number;
}>;

type LifecycleV3Manifest = Readonly<{
  schemaVersion: 1;
  current: LifecycleV3GenerationReference;
  previous: LifecycleV3GenerationReference | null;
}>;

export interface OriginalRecordingPublicationInput {
  startedEvent: AnyMeetingStartedLifecycleEvent;
  terminalEvent: AnyMeetingTerminalLifecycleEvent;
  sourceFileBase: string;
  lifecycleV3Snapshot?: DurableCraigLifecycleV3Snapshot;
}

export interface InterruptedOriginalRecordingRecoveryInput {
  recordingId: string;
  guildId: string;
  channelId: string;
  startedAt: string;
  recoveredAt: string;
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
  schemaVersion: 2 | 3;
  publicationId: string;
  recordingId: string;
  guildId: string;
  channelId: string;
  startedEvent: AnyMeetingStartedLifecycleEvent;
  terminalEvent: AnyMeetingTerminalLifecycleEvent;
  lifecycleV3Snapshot?: DurableCraigLifecycleV3Snapshot;
  admissionDigestSha256?: string;
  sourceFiles: OriginalRecordingSourceFileReference[];
  authoritativeTracks?: PreparedAuthoritativeTrack[];
  authoritativeTimelineBasis?: typeof authoritativeTimelineBasis;
}

interface PendingOriginalRecordingJob {
  filePath: string;
  job: OriginalRecordingOutboxJob;
}

interface PendingCancellationProofJob {
  filePath: string;
  consecutiveFailures: number;
  notBeforeMs: number;
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
  schemaVersion: 1 | 3;
  eventId: string;
  recordingId: string;
  guildId: string;
  channelId: string;
  occurredAt: string;
  type: 'recording.authoritative_ready';
  endedAt: string;
  trackCount: number;
  sourceFilesChecksumSha256: string;
  actorObservationState?: 'consistent' | 'conflicted';
  actorSemanticsVersion?: 1;
  producerCapabilityId?: string;
  producerRevision?: string;
  actors?: Array<{ actorId: string; kind: 'human' | 'automation' | 'unknown' }>;
  rosterState?: 'sealed';
}

export interface CookedAuthoritativeTrack {
  filePath: string;
  checksumSha256: string;
  sizeBytes: number;
  dispose(): Promise<void>;
}

export interface DurableAuthoritativeTrackUploadAcknowledgement {
  schemaVersion: 1;
  uploadId: string;
  recordingId: string;
  trackNumber: number;
  checksumSha256: string;
  sizeBytes: number;
  durable: true;
  immutable: true;
  object: { provider: 's3'; bucket: string; key: string; versionId: string };
}

export interface CancellationPcmFenceProofReceipt {
  schemaVersion: 1;
  proofId: string;
  object: DurableAuthoritativeTrackUploadAcknowledgement['object'];
  sizeBytes: number;
  checksumSha256: string;
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
  postAuthoritativeTrack?(metadata: AuthoritativeTrackMetadata, audioFilePath: string): Promise<DurableAuthoritativeTrackUploadAcknowledgement>;
  postAuthoritativeReady?(event: AuthoritativeRecordingReadyEvent): Promise<void>;
  postCancellationPcmFence?(proof: CraigAuthoritativeCancellationPcmFenceLog): Promise<CancellationPcmFenceProofReceipt>;
}

export interface MeetingIntegrationSink {
  readonly lifecycleProducerConfiguration: MeetingLifecycleProducerConfiguration;
  publishLifecycle(event: MeetingLifecycleEvent, lifecycleV3Admission?: CraigLifecycleV3Admission): MeetingLifecyclePublishOutcome;
  publishPacket(packet: MeetingVoicePacket, opus: Buffer): boolean;
  publishOriginalRecording(input: OriginalRecordingPublicationInput): Promise<boolean>;
  recoverInterruptedOriginalRecording(input: InterruptedOriginalRecordingRecoveryInput): Promise<boolean>;
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

export type MeetingLifecyclePublishOutcome =
  | Readonly<{ status: 'accepted' }>
  | Readonly<{ status: 'missing-start' }>
  | Readonly<{ status: 'duplicate' }>
  | Readonly<{ status: 'persistence-failed' }>
  | Readonly<{ status: 'capacity-exhausted' }>;

const acceptedLifecycleOutcome: MeetingLifecyclePublishOutcome = Object.freeze({ status: 'accepted' });

export function reportMeetingLifecyclePublishOutcome(
  logger: MeetingIntegrationLogger,
  recordingId: string,
  eventType: MeetingLifecycleEvent['type'],
  outcome: MeetingLifecyclePublishOutcome
): void {
  switch (outcome.status) {
    case 'accepted':
      return;
    case 'capacity-exhausted':
      logger.error(`Meeting integration lifecycle queue is full for recording ${recordingId} (${eventType})`);
      return;
    case 'missing-start':
      logger.warn(`Meeting integration rejected lifecycle event without an accepted start for recording ${recordingId} (${eventType})`);
      return;
    case 'duplicate':
      logger.debug(`Meeting integration ignored duplicate lifecycle event for recording ${recordingId} (${eventType})`);
      return;
    case 'persistence-failed':
      logger.error(`Meeting integration could not durably persist lifecycle evidence for recording ${recordingId} (${eventType})`);
  }
}

export class MeetingTerminalLifecycle {
  private started = false;
  private published = false;

  acceptStart(outcome: MeetingLifecyclePublishOutcome): boolean {
    if (outcome.status !== 'accepted') return false;
    this.started = true;
    return true;
  }

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
    if (!this.started || this.published) return;
    this.published = true;
    publish(type);
  }
}

export class NoopMeetingIntegrationSink implements MeetingIntegrationSink {
  readonly lifecycleProducerConfiguration = Object.freeze({ schemaVersion: 1 as const });
  publishLifecycle(): MeetingLifecyclePublishOutcome {
    return acceptedLifecycleOutcome;
  }

  publishPacket(): boolean {
    return true;
  }

  async publishOriginalRecording(): Promise<boolean> {
    return true;
  }

  async recoverInterruptedOriginalRecording(): Promise<boolean> {
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
  readonly lifecycleProducerConfiguration: MeetingLifecycleProducerConfiguration;
  private queue: QueueItem[] = [];
  private queueHead = 0;
  private readonly originalJobs: PendingOriginalRecordingJob[] = [];
  private readonly drainWaiters = new Set<() => void>();
  private readonly openRecordings = new Set<string>();
  private readonly closedRecordings = new Map<string, true>();
  private processing = false;
  private retryTimer: NodeJS.Timeout | null = null;
  private processingOriginal = false;
  private originalRetryTimer: NodeJS.Timeout | null = null;
  private lifecycleV3MaintenanceTimer: NodeJS.Timeout | null = null;
  private lifecycleV3MaintenanceFailures = 0;
  private processingCancellationProof = false;
  private cancellationProofRetryTimer: NodeJS.Timeout | null = null;
  private readonly cancellationProofJobs: PendingCancellationProofJob[] = [];
  private readonly cancellationProofPaths = new Set<string>();
  private queuedPackets = 0;
  private consecutiveFailures = 0;
  private consecutiveOriginalFailures = 0;
  private readonly recordingRoot?: string;
  private readonly pendingOriginalRoot?: string;
  private readonly rejectedOriginalRoot?: string;
  private readonly pendingLifecycleV3Root?: string;
  private readonly rejectedLifecycleV3Root?: string;
  private readonly originalCooker?: OriginalRecordingCooker;
  /** Validated once on recovery, then maintained in O(1) on admission/ACK. */
  private readonly lifecycleV3JournalIndex = new Map<string, LifecycleV3JournalState | undefined>();
  /** Journals are inserted/deleted in O(1); maintenance never scans all journals. */
  private readonly lifecycleV3MaintenanceQueue = new Set<string>();

  constructor(
    private readonly transport: MeetingIntegrationTransport,
    private readonly logger: MeetingIntegrationLogger,
    private readonly maxQueuedPackets = 8192,
    private readonly batchSize = 128,
    private readonly maxQueuedLifecycleEvents = 1024,
    originalRecording?: OriginalRecordingOutboxOptions,
    lifecycleProducerConfiguration: unknown = { schemaVersion: 1 }
  ) {
    this.lifecycleProducerConfiguration = parseMeetingLifecycleProducerConfiguration(lifecycleProducerConfiguration);
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
      this.pendingLifecycleV3Root = path.join(outboxRoot, 'lifecycle-v3', 'pending');
      this.rejectedLifecycleV3Root = path.join(outboxRoot, 'lifecycle-v3', 'rejected');
      this.originalCooker = originalRecording.cooker ?? new CraigOriginalRecordingCooker();
    }
  }

  publishLifecycle(event: MeetingLifecycleEvent, lifecycleV3Admission?: CraigLifecycleV3Admission): MeetingLifecyclePublishOutcome {
    // Lifecycle traffic is tiny and must remain ordered with the accepted audio.
    const queuedLifecycleEvents = this.queueLength() - this.queuedPackets;
    const isTerminal = event.type === 'meeting.ended' || event.type === 'meeting.aborted';
    if (event.type === 'meeting.started') {
      if (this.openRecordings.has(event.recordingId) || this.closedRecordings.has(event.recordingId)) return { status: 'duplicate' };
      if (queuedLifecycleEvents + this.openRecordings.size + 2 > this.maxQueuedLifecycleEvents) return { status: 'capacity-exhausted' };
    } else if (!this.openRecordings.has(event.recordingId))
      return { status: isTerminal && this.closedRecordings.has(event.recordingId) ? 'duplicate' : 'missing-start' };
    else if (!isTerminal && queuedLifecycleEvents + this.openRecordings.size + 1 > this.maxQueuedLifecycleEvents)
      return { status: 'capacity-exhausted' };

    if (event.schemaVersion === 3) {
      try {
        this.persistLifecycleV3Admission(event, lifecycleV3Admission);
      } catch (error) {
        this.logger.error(`Failed to persist lifecycle v3 evidence for ${event.recordingId}`, error);
        return { status: 'persistence-failed' };
      }
    }

    if (event.type === 'meeting.started') this.openRecordings.add(event.recordingId);
    this.queue.push({ type: 'lifecycle', event });
    if (isTerminal) {
      this.openRecordings.delete(event.recordingId);
      this.rememberClosedRecording(event.recordingId);
    }
    this.scheduleProcessing();
    return acceptedLifecycleOutcome;
  }

  private rememberClosedRecording(recordingId: string): void {
    this.closedRecordings.set(recordingId, true);
    if (this.closedRecordings.size <= this.maxQueuedLifecycleEvents) return;

    const oldestRecordingId = this.closedRecordings.keys().next().value;
    if (oldestRecordingId !== undefined) this.closedRecordings.delete(oldestRecordingId);
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
      else if (existing.admissionDigestSha256 !== job.admissionDigestSha256) {
        await this.rejectOriginalJob(filePath, path.basename(filePath));
        throw new Error(`Original recording outbox contains a conflicting job for ${job.recordingId}`);
      }
      this.enqueueOriginalJob({ filePath, job: queuedJob });
      this.scheduleOriginalProcessing();
      return true;
    } catch (error) {
      this.logger.error(`Failed to persist original recording outbox job for ${recordingId}; original Craig files remain authoritative`, error);
      return false;
    }
  }

  async recoverInterruptedOriginalRecording(input: InterruptedOriginalRecordingRecoveryInput): Promise<boolean> {
    if (this.recordingRoot === undefined) {
      this.logger.warn(`Meeting original recording recovery is not configured for ${input.recordingId}`);
      return false;
    }

    try {
      assertOriginalSourceFileBase(input.recordingId, this.recordingRoot, input.sourceFileBase);
      const users = await inspectOriginalRecordingUsers(`${input.sourceFileBase}.users`);
      const botSpeakerId = await inspectOriginalRecordingBotSpeakerId(`${input.sourceFileBase}.info`);
      await inspectOriginalRecordingData(
        `${input.sourceFileBase}.data`,
        users.tracks.map(({ trackNumber }) => trackNumber)
      );
      await Promise.all(sourceFileKinds.map((kind) => digestFile(`${input.sourceFileBase}.${kind}`)));
      const recoveryIdentity = createHash('sha256').update(input.recordingId).digest('hex').slice(0, 32);

      const durableLifecycle = this.readLifecycleV3Snapshot(input.recordingId);
      if (durableLifecycle !== undefined) {
        const lifecycle = restoreCraigLifecycleV3ProducerFromSnapshot(durableLifecycle);
        if (
          durableLifecycle.guildId !== input.guildId ||
          durableLifecycle.channelId !== input.channelId ||
          durableLifecycle.recordingId !== input.recordingId
        )
          throw new Error('Durable lifecycle v3 recovery evidence belongs to another recording context');
        const envelope = (eventId: string, occurredAt: string) => ({
          eventId,
          recordingId: input.recordingId,
          guildId: input.guildId,
          channelId: input.channelId,
          occurredAt
        });
        const startedEvent = durableLifecycle.pendingOutbox.find((event) => event.type === 'meeting.started');
        if (startedEvent?.type !== 'meeting.started') throw new Error('Durable lifecycle v3 recovery evidence has no trusted start');
        const persistedTerminal = durableLifecycle.pendingOutbox.find((event) => event.type === 'meeting.ended' || event.type === 'meeting.aborted');
        const terminalEvent =
          persistedTerminal?.type === 'meeting.ended' || persistedTerminal?.type === 'meeting.aborted'
            ? persistedTerminal
            : lifecycle.terminal(
                envelope(`recovery:v3:${recoveryIdentity}:ended`, input.recoveredAt),
                'meeting.ended',
                'Craig restarted during an active recording; the authoritative original was recovered from durable files.'
              );
        const recoveredSnapshot = lifecycle.durableSnapshot();
        this.persistLifecycleV3Snapshot(recoveredSnapshot);
        return await this.publishOriginalRecording({
          sourceFileBase: input.sourceFileBase,
          startedEvent,
          terminalEvent,
          lifecycleV3Snapshot: recoveredSnapshot
        });
      }

      return await this.publishOriginalRecording({
        sourceFileBase: input.sourceFileBase,
        startedEvent: {
          schemaVersion: 1,
          eventId: `recovery:v1:${recoveryIdentity}:started`,
          recordingId: input.recordingId,
          guildId: input.guildId,
          channelId: input.channelId,
          occurredAt: input.startedAt,
          type: 'meeting.started',
          participantIds: users.tracks.filter(({ speakerId }) => speakerId !== botSpeakerId).map(({ speakerId }) => speakerId)
        },
        terminalEvent: {
          schemaVersion: 1,
          eventId: `recovery:v1:${recoveryIdentity}:ended`,
          recordingId: input.recordingId,
          guildId: input.guildId,
          channelId: input.channelId,
          occurredAt: input.recoveredAt,
          type: 'meeting.ended',
          reason: 'Craig restarted during an active recording; the authoritative original was recovered from durable files.'
        }
      });
    } catch (error) {
      this.logger.error(
        `Failed to recover interrupted original Craig recording ${input.recordingId}; original files and database evidence remain available`,
        error
      );
      return false;
    }
  }

  async restoreOriginalRecordingJobs(): Promise<void> {
    if (this.pendingOriginalRoot === undefined || this.rejectedOriginalRoot === undefined) return;
    try {
      await this.ensureOriginalOutboxDirectories();
    } catch (error) {
      this.logger.error('Failed to initialize Meeting original recording outbox directories; original Craig files remain untouched', error);
      return;
    }
    try {
      this.restoreCancellationProofJobs();
      this.scheduleCancellationProofProcessing();
    } catch (error) {
      this.logger.error('Failed to scan cancellation PCM proof outbox; proof evidence remains untouched', error);
    }
    try {
      this.restoreLifecycleV3Admissions();
      this.scheduleLifecycleV3Maintenance();
      const entries = (await readdir(this.pendingOriginalRoot)).filter((entry) => entry.endsWith('.json')).sort();
      for (const entry of entries) {
        const filePath = path.join(this.pendingOriginalRoot, entry);
        try {
          const job = await readOriginalRecordingJob(filePath);
          this.enqueueOriginalJob({
            filePath,
            job
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

  private async replayCancellationProofs(): Promise<void> {
    if (this.pendingOriginalRoot === undefined || this.transport.postCancellationPcmFence === undefined) return;
    const root = path.join(this.pendingOriginalRoot, 'cancellation-pcm-fence');
    mkdirSync(root, { recursive: true, mode: 0o700 });
    for (const entry of readdirSync(root).filter((name) => name.endsWith('.json')).sort()) {
      await this.deliverCancellationProof(path.join(root, entry));
    }
  }

  private restoreCancellationProofJobs(): void {
    if (this.pendingOriginalRoot === undefined || this.transport.postCancellationPcmFence === undefined) return;
    const root = path.join(this.pendingOriginalRoot, 'cancellation-pcm-fence');
    mkdirSync(root, { recursive: true, mode: 0o700 });
    for (const entry of readdirSync(root).filter((name) => name.endsWith('.json')).sort())
      this.enqueueCancellationProof(path.join(root, entry));
  }

  private enqueueCancellationProof(filePath: string): void {
    if (this.cancellationProofPaths.has(filePath)) return;
    this.cancellationProofPaths.add(filePath);
    this.cancellationProofJobs.push({ filePath, consecutiveFailures: 0, notBeforeMs: 0 });
  }

  private scheduleCancellationProofProcessing(delayMs = 0): void {
    if (this.processingCancellationProof || this.cancellationProofRetryTimer || this.cancellationProofJobs.length === 0 ||
        this.transport.postCancellationPcmFence === undefined) return;
    this.cancellationProofRetryTimer = setTimeout(() => {
      this.cancellationProofRetryTimer = null;
      void this.processCancellationProof();
    }, delayMs);
  }

  private async processCancellationProof(): Promise<void> {
    if (this.processingCancellationProof || this.cancellationProofJobs.length === 0) return;
    const now = Date.now();
    let pending: PendingCancellationProofJob | undefined;
    let nextDelayMs = Number.POSITIVE_INFINITY;
    const selectionCount = Math.min(cancellationProofSelectionBudget, this.cancellationProofJobs.length);
    for (let index = 0; index < selectionCount; index++) {
      const candidate = this.cancellationProofJobs.shift()!;
      if (pending === undefined && candidate.notBeforeMs <= now) pending = candidate;
      else {
        nextDelayMs = Math.min(nextDelayMs, Math.max(0, candidate.notBeforeMs - now));
        this.cancellationProofJobs.push(candidate);
      }
    }
    if (pending === undefined) {
      this.scheduleCancellationProofProcessing(Number.isFinite(nextDelayMs) ? nextDelayMs : retryDelay(1));
      return;
    }
    this.processingCancellationProof = true;
    try {
      await this.deliverCancellationProof(pending.filePath);
      this.cancellationProofPaths.delete(pending.filePath);
    } catch (error) {
      if (isRetryableDeliveryError(error)) {
        pending.consecutiveFailures++;
        const delayMs = retryDelay(pending.consecutiveFailures);
        pending.notBeforeMs = Date.now() + delayMs;
        this.cancellationProofJobs.push(pending);
        this.logger.error(`Cancellation PCM proof delivery failed; retrying in ${delayMs}ms`, error);
      } else {
        this.cancellationProofPaths.delete(pending.filePath);
        this.logger.error('Cancellation PCM proof was permanently rejected; retaining durable proof for operator recovery', error);
      }
    }
    this.processingCancellationProof = false;
    this.scheduleCancellationProofProcessing();
    this.notifyIfDrained();
  }

  private async deliverCancellationProof(proofPath: string): Promise<void> {
    if (this.pendingOriginalRoot === undefined || this.transport.postCancellationPcmFence === undefined) return;
    const proof = JSON.parse(readFileSync(proofPath, 'utf8')) as CraigAuthoritativeCancellationPcmFenceLog;
    const normalized = createAuthoritativeCancellationPcmFenceLog(proof);
    const manifestPath = path.join(this.pendingOriginalRoot, 'cancellation-pcm-fence-manifests', path.basename(proofPath));
    const manifest = readCancellationProofManifest(manifestPath, normalized);
    const receipt = await this.transport.postCancellationPcmFence(normalized);
    assertCancellationProofReceipt(receipt, normalized, manifest.uploadAcknowledgement);
    unlinkSync(proofPath);
    syncDirectorySync(path.dirname(proofPath));
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
      mkdir(this.rejectedOriginalRoot!, { recursive: true, mode: 0o700 }),
      mkdir(this.pendingLifecycleV3Root!, { recursive: true, mode: 0o700 }),
      mkdir(this.rejectedLifecycleV3Root!, { recursive: true, mode: 0o700 })
    ]);
  }

  private persistLifecycleV3Admission(event: CraigLifecycleV3Event, admission: CraigLifecycleV3Admission | undefined): void {
    const journal = this.readLifecycleV3Journal(event.recordingId);
    if (journal !== undefined) {
      const existingDigest = journal.eventDigests.get(event.eventId);
      if (existingDigest !== undefined) {
        if (existingDigest === createHash('sha256').update(canonicalJson(event)).digest('hex')) return;
        throw new Error('Lifecycle v3 durable admission conflicts with an existing event');
      }
      if (Date.parse(event.occurredAt) < Date.parse(journal.lastOccurredAt))
        throw new Error('Lifecycle v3 durable admission is not an ordered extension');
      if (journal.nextSequence - journal.ackedSequence - 1 >= this.maxQueuedLifecycleEvents)
        throw new Error('Lifecycle v3 durable journal capacity is exhausted');
    } else {
      if (admission === undefined) throw new Error('First lifecycle v3 admission requires its durable producer state');
      const configured = this.lifecycleProducerConfiguration;
      // The first one-event snapshot is the only admission-time full restore.
      // It establishes the trusted index; all growing snapshots thereafter are
      // checked against that index without rescanning their emitted prefix.
      if (
        event.type !== 'meeting.started' ||
        configured.schemaVersion !== 3 ||
        configured.actorSemanticsVersion !== admission.producer.actorSemanticsVersion ||
        configured.producerCapabilityId !== admission.producer.producerCapabilityId ||
        configured.producerRevision !== admission.producer.producerRevision
      )
        throw new Error('Lifecycle v3 durable admission does not match the active producer rollout');
    }
    if (admission !== undefined && (admission.recordingId !== event.recordingId || admission.guildId !== event.guildId || admission.channelId !== event.channelId))
      throw new Error('Lifecycle v3 durable admission context is inconsistent');
    if (admission !== undefined && journal !== undefined &&
        (canonicalJson(admission.producer) !== canonicalJson(journal.snapshot.producer) ||
         admission.recordingId !== journal.snapshot.recordingId || admission.guildId !== journal.snapshot.guildId ||
         admission.channelId !== journal.snapshot.channelId))
      throw new Error('Lifecycle v3 durable admission changed its indexed producer identity');
    this.appendLifecycleV3Event(admission, event, journal?.nextSequence ?? 0);
  }

  /** One fsynced immutable segment per admission; the bounded checkpoint never contains the growing event prefix. */
  private appendLifecycleV3Event(admission: CraigLifecycleV3Admission | undefined, event: CraigLifecycleV3Event, sequence: number): void {
    const previous = this.lifecycleV3JournalIndex.get(event.recordingId);
    if (previous === undefined && admission === undefined) throw new Error('Lifecycle v3 journal has no base generation');
    const root = this.lifecycleV3JournalRoot(event.recordingId);
    mkdirSync(root, { recursive: true, mode: 0o700 });
    const segment = path.join(root, `${String(sequence).padStart(8, '0')}.event.json`);
    if (existsSync(segment)) {
      const durableEvent = JSON.parse(readFileSync(segment, 'utf8')) as CraigLifecycleV3Event;
      if (canonicalJson(durableEvent) !== canonicalJson(event))
        throw new Error('Lifecycle v3 immutable admission segment conflicts with its retry');
    } else this.writeDurableJson(segment, event);
    if (previous === undefined) this.publishLifecycleV3Generation(root, {
      schemaVersion: 2, generation: 0, recordingId: admission!.recordingId, guildId: admission!.guildId,
      channelId: admission!.channelId, producer: admission!.producer, baseSequence: 0,
      actorObservationState: admission!.actorObservationState, actors: admission!.actors, sealedReady: admission!.sealedReady
    }, 0);
    const pendingEvents = previous?.pendingEvents ?? new Map<number, CraigLifecycleV3Event>();
    pendingEvents.set(sequence, event);
    const eventDigests = previous?.eventDigests ?? new Map<string, string>();
    eventDigests.set(event.eventId, createHash('sha256').update(canonicalJson(event)).digest('hex'));
    const priorSnapshot = previous?.snapshot;
    const actorIndex = previous?.actorIndex ?? new Map(admission!.actors.map((actor) => [actor.actorId, actor]));
    if (event.type === 'participant.joined') actorIndex.set(event.actor.actorId, event.actor);
    this.lifecycleV3JournalIndex.set(event.recordingId, {
      snapshot: {
        schemaVersion: 2,
        recordingId: event.recordingId, guildId: event.guildId, channelId: event.channelId,
        producer: { actorSemanticsVersion: event.actorSemanticsVersion, producerCapabilityId: event.producerCapabilityId, producerRevision: event.producerRevision },
        actorObservationState: event.actorObservationState, actors: priorSnapshot?.actors ?? admission!.actors,
        sealedReady: event.type === 'recording.authoritative_ready' ? event : priorSnapshot?.sealedReady ?? null,
        emitted: previous?.snapshot.emitted ?? [],
        pendingOutbox: previous?.snapshot.pendingOutbox ?? []
      },
      actorIndex,
      pendingEvents,
      eventDigests,
      generation: previous?.generation ?? 0,
      nextSequence: sequence + 1,
      ackedSequence: previous?.ackedSequence ?? -1,
      closed: previous?.closed === true || event.type === 'meeting.ended' || event.type === 'meeting.aborted',
      lastOccurredAt: event.occurredAt,
      lastAcknowledgedEventId: previous?.lastAcknowledgedEventId ?? null,
      lastAcknowledgedDigest: previous?.lastAcknowledgedDigest ?? null,
      maintenanceNeeded: previous?.maintenanceNeeded ?? false
    });
  }

  private writeDurableJson(filePath: string, value: unknown): void {
    const temporaryPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
    const descriptor = openSync(temporaryPath, 'wx', 0o600);
    try { writeFileSync(descriptor, `${JSON.stringify(value)}\n`, 'utf8'); fsyncSync(descriptor); }
    finally { closeSync(descriptor); }
    renameSync(temporaryPath, filePath);
    syncDirectorySync(path.dirname(filePath));
  }

  private publishLifecycleV3Generation(root: string, payload: Record<string, unknown>, coveredDeltaCursor: number): void {
    const generation = Number(payload.generation);
    const payloadJson = canonicalJson(payload);
    const checksumSha256 = createHash('sha256').update(payloadJson).digest('hex');
    const file = `snapshot-${String(generation).padStart(8, '0')}.json`;
    const generationPath = path.join(root, file);
    if (!existsSync(generationPath)) this.writeDurableJson(generationPath, {
      schemaVersion: 3, generation, coveredDeltaCursor, checksumSha256, payload
    });
    else this.readLifecycleV3Generation(root, { generation, file, coveredDeltaCursor, checksumSha256 });

    const manifestPath = path.join(root, 'manifest.json');
    const oldManifest = this.tryReadLifecycleV3Manifest(root);
    const current = { generation, file, checksumSha256, coveredDeltaCursor };
    const previous = oldManifest?.current && oldManifest.current.generation !== generation ? oldManifest.current : oldManifest?.previous ?? null;
    const nextManifest: LifecycleV3Manifest = { schemaVersion: 1, current, previous };
    // Initial generation also receives an independently fsynced valid fallback
    // before current publication, so a torn first manifest remains recoverable.
    this.writeDurableJson(path.join(root, 'manifest.previous.json'), oldManifest ?? nextManifest);
    this.writeDurableJson(manifestPath, nextManifest);

    const keep = new Set([current.file, previous?.file].filter((name): name is string => name !== undefined));
    for (const entry of readdirSync(root))
      if (/^snapshot-\d{8}\.json$/.test(entry) && !keep.has(entry)) unlinkSync(path.join(root, entry));
    syncDirectorySync(root);
  }

  private readLifecycleV3Generation(root: string, reference: LifecycleV3GenerationReference): any {
    if (reference.file !== `snapshot-${String(reference.generation).padStart(8, '0')}.json`)
      throw new Error('Lifecycle v3 snapshot generation identity is malformed');
    const value = JSON.parse(readFileSync(path.join(root, reference.file), 'utf8')) as any;
    if (value.schemaVersion === 4) {
      const descriptor = {
        schemaVersion: 4, generation: value.generation, coveredDeltaCursor: value.coveredDeltaCursor,
        chunkCount: value.chunkCount, chunksChecksumSha256: value.chunksChecksumSha256
      };
      if (value.generation !== reference.generation || value.coveredDeltaCursor !== reference.coveredDeltaCursor ||
          value.checksumSha256 !== reference.checksumSha256 ||
          createHash('sha256').update(canonicalJson(descriptor)).digest('hex') !== reference.checksumSha256 ||
          !Number.isSafeInteger(value.chunkCount) || value.chunkCount < 1)
        throw new Error('Lifecycle v3 chunked generation descriptor is invalid');
      let rollingChecksum = '';
      let payload: any;
      const actorIndex = new Map<string, any>();
      let pendingEvent: any;
      for (let index = 0; index < value.chunkCount; index++) {
        const file = `generation-${String(value.generation).padStart(8, '0')}-chunk-${String(index).padStart(8, '0')}.json`;
        const chunk = JSON.parse(readFileSync(path.join(root, file), 'utf8')) as any;
        const chunkPayload = { schemaVersion: chunk.schemaVersion, generation: chunk.generation, index: chunk.index, records: chunk.records };
        const checksum = createHash('sha256').update(canonicalJson(chunkPayload)).digest('hex');
        if (chunk.schemaVersion !== 1 || chunk.generation !== value.generation || chunk.index !== index ||
            checksum !== chunk.checksumSha256 || !Array.isArray(chunk.records) ||
            chunk.records.length < 1 || chunk.records.length > lifecycleV3MaintenanceRecordsPerStep)
          throw new Error('Lifecycle v3 immutable generation chunk is invalid');
        rollingChecksum = createHash('sha256').update(`${rollingChecksum}:${checksum}`).digest('hex');
        for (const record of chunk.records) {
          if (record.kind === 'base') { payload = record.value; pendingEvent = undefined; }
          else if (record.kind === 'actor') {
            actorIndex.set(record.value.actorId, record.value);
            if (payload.sealedReady?.actors !== undefined) payload.sealedReady.actors.push(record.value);
          }
          else if (record.kind === 'event') {
            pendingEvent = record.value;
            payload.actorObservationState = pendingEvent.actorObservationState;
            if (pendingEvent.type === 'meeting.started' || pendingEvent.type === 'recording.authoritative_ready') actorIndex.clear();
            if (pendingEvent.type === 'recording.authoritative_ready') payload.sealedReady = pendingEvent;
          } else if (record.kind === 'event-actor') {
            actorIndex.set(record.value.actorId, record.value);
            if (pendingEvent?.type === 'recording.authoritative_ready') payload.sealedReady.actors.push(record.value);
          }
        }
      }
      if (payload === undefined || rollingChecksum !== value.chunksChecksumSha256)
        throw new Error('Lifecycle v3 chunked generation checksum is invalid');
      payload.generation = value.generation;
      payload.baseSequence = value.coveredDeltaCursor;
      payload.actors = [...actorIndex.values()].sort((left, right) => left.actorId.localeCompare(right.actorId));
      return payload;
    }
    if (value.schemaVersion !== 3 || value.generation !== reference.generation ||
        value.coveredDeltaCursor !== reference.coveredDeltaCursor || value.checksumSha256 !== reference.checksumSha256 ||
        createHash('sha256').update(canonicalJson(value.payload)).digest('hex') !== reference.checksumSha256)
      throw new Error('Lifecycle v3 snapshot generation checksum is invalid');
    return value.payload;
  }

  private tryReadLifecycleV3Manifest(root: string): LifecycleV3Manifest | undefined {
    try {
      const value = JSON.parse(readFileSync(path.join(root, 'manifest.json'), 'utf8')) as any;
      if (value.schemaVersion !== 1 || !this.isLifecycleV3GenerationReference(value.current) ||
          (value.previous !== null && !this.isLifecycleV3GenerationReference(value.previous)))
        return undefined;
      return value as LifecycleV3Manifest;
    } catch { return undefined; }
  }

  private isLifecycleV3GenerationReference(value: unknown): value is LifecycleV3GenerationReference {
    return isRecord(value) && Number.isSafeInteger(value.generation) && Number(value.generation) >= 0 &&
      value.file === `snapshot-${String(value.generation).padStart(8, '0')}.json` &&
      typeof value.checksumSha256 === 'string' && /^[a-f0-9]{64}$/.test(value.checksumSha256) &&
      Number.isSafeInteger(value.coveredDeltaCursor) && Number(value.coveredDeltaCursor) >= 0;
  }

  private recoverLifecycleV3Generation(root: string): any | undefined {
    const manifest = this.tryReadLifecycleV3Manifest(root);
    for (const [index, reference] of [manifest?.current, manifest?.previous].entries()) {
      if (reference === undefined || reference === null) continue;
      try {
        const payload = this.readLifecycleV3Generation(root, reference);
        if (index > 0) {
          this.writeDurableJson(path.join(root, 'manifest.json'), { schemaVersion: 1, current: reference, previous: null });
          if (manifest?.current.file !== reference.file && /^snapshot-\d{8}\.json$/.test(manifest!.current.file) &&
              existsSync(path.join(root, manifest!.current.file))) {
            unlinkSync(path.join(root, manifest!.current.file));
            syncDirectorySync(root);
          }
        }
        return payload;
      } catch { /* deterministic fallback */ }
    }
    // A torn current manifest may only fall back to the separately fsynced prior
    // manifest. Unreferenced descriptors/chunks are never eligible for recovery.
    try {
      const prior = JSON.parse(readFileSync(path.join(root, 'manifest.previous.json'), 'utf8')) as any;
      if (prior.schemaVersion !== 1 || !this.isLifecycleV3GenerationReference(prior.current) ||
          (prior.previous !== null && !this.isLifecycleV3GenerationReference(prior.previous))) return undefined;
      for (const reference of [prior.current, prior.previous]) if (reference !== null) try {
        const payload = this.readLifecycleV3Generation(root, reference);
        this.writeDurableJson(path.join(root, 'manifest.json'), { schemaVersion: 1, current: reference, previous: null });
        return payload;
      } catch { /* verify the referenced fallback */ }
    } catch { /* no complete manifest-referenced generation */ }
    return undefined;
  }

  private lifecycleV3JournalRoot(recordingId: string): string {
    if (this.pendingLifecycleV3Root === undefined) throw new Error('Lifecycle v3 durable outbox is not configured');
    assertRecordingId(recordingId);
    return path.join(this.pendingLifecycleV3Root, `${recordingId}.journal`);
  }

  private readLifecycleV3Journal(recordingId: string): LifecycleV3JournalState | undefined {
    if (this.lifecycleV3JournalIndex.has(recordingId)) return this.lifecycleV3JournalIndex.get(recordingId);
    const journal = this.readLifecycleV3JournalUncached(recordingId);
    this.lifecycleV3JournalIndex.set(recordingId, journal);
    if (journal?.maintenanceNeeded === true) this.lifecycleV3MaintenanceQueue.add(recordingId);
    return journal;
  }

  private readLifecycleV3JournalUncached(recordingId: string): LifecycleV3JournalState | undefined {
    const root = this.lifecycleV3JournalRoot(recordingId);
    if (!existsSync(root)) return undefined;
    const checkpoint = this.recoverLifecycleV3Generation(root);
    if (checkpoint === undefined) return undefined;
    const entries = readdirSync(root).filter((name) => /^\d{8}\.event\.json$/.test(name)).sort();
    const cursorPath = path.join(root, 'cursor.json');
    const cursor = existsSync(cursorPath) ? JSON.parse(readFileSync(cursorPath, 'utf8')) as any : null;
    const ackedSequence = cursor?.ackedSequence ?? -1;
    const nextSequence = entries.length === 0 ? 0 : Number(entries[entries.length - 1].slice(0, 8)) + 1;
    if (checkpoint.schemaVersion !== 2 || !Number.isSafeInteger(checkpoint.generation) || checkpoint.generation < 0 ||
        (cursor !== null && (!Number.isSafeInteger(cursor.generation) || cursor.generation < 0)) ||
        !Number.isSafeInteger(ackedSequence) || ackedSequence < -1 || ackedSequence >= nextSequence)
      throw new Error('Lifecycle v3 journal checkpoint is malformed');
    const retained = entries.map((entry) => ({
      sequence: Number(entry.slice(0, 8)),
      event: JSON.parse(readFileSync(path.join(root, entry), 'utf8')) as CraigLifecycleV3Event
    }));
    if (retained.length === 0 || retained[0].sequence !== 0 || retained[0].event.type !== 'meeting.started')
      throw new Error('Lifecycle v3 journal lost its pinned start segment');
    if (ackedSequence >= 0) {
      const acked = retained.find(({ sequence }) => sequence === ackedSequence);
      if (!isRecord(cursor) || typeof cursor.eventId !== 'string' || typeof cursor.digestSha256 !== 'string' || acked === undefined ||
          cursor.eventId !== acked.event.eventId || cursor.digestSha256 !== createHash('sha256').update(canonicalJson(acked.event)).digest('hex'))
        throw new Error('Lifecycle v3 journal acknowledgement cursor does not match its durable segment');
    } else if (cursor !== null) {
      throw new Error('Lifecycle v3 journal has an unexpected acknowledgement cursor');
    }
    const retainedSequences = new Set(retained.map(({ sequence }) => sequence));
    for (let sequence = Math.max(ackedSequence + 1, 1); sequence < nextSequence; sequence++)
      if (!retainedSequences.has(sequence)) throw new Error('Lifecycle v3 journal lost an unacknowledged segment');
    const pendingEvents = new Map(retained.filter(({ sequence }) => sequence > ackedSequence).map(({ sequence, event }) => [sequence, event]));
    const pendingValues = [...pendingEvents.values()];
    const events = [retained[0].event, ...pendingValues.filter(({ eventId }) => eventId !== retained[0].event.eventId)];
    const sealedReady = events.find(({ type }) => type === 'recording.authoritative_ready') ?? checkpoint.sealedReady ?? null;
    const baseSequence = Number.isSafeInteger(checkpoint.baseSequence) ? checkpoint.baseSequence : 0;
    const actorIndex = new Map<string, DurableCraigLifecycleV3Snapshot['actors'][number]>();
    for (const actor of checkpoint.actors ?? []) actorIndex.set(actor.actorId, actor);
    for (const { sequence, event } of retained) if (sequence > baseSequence) {
      if (event.type === 'meeting.started' || event.type === 'recording.authoritative_ready') {
        actorIndex.clear();
        for (const actor of event.actors) actorIndex.set(actor.actorId, actor);
      } else if (event.type === 'participant.joined') actorIndex.set(event.actor.actorId, event.actor);
    }
    const actors = [...actorIndex.values()].sort((left, right) => left.actorId.localeCompare(right.actorId));
    const last = retained[retained.length - 1].event;
    const snapshot = restoreCraigLifecycleV3ProducerFromSnapshot({
      schemaVersion: 2, recordingId: checkpoint.recordingId, guildId: checkpoint.guildId, channelId: checkpoint.channelId,
      producer: checkpoint.producer, actorObservationState: last.actorObservationState ?? checkpoint.actorObservationState, actors,
      emitted: events.map(({ eventId, occurredAt }) => ({ eventId, occurredAt })), pendingOutbox: events,
      sealedReady
    }).durableSnapshot();
    return {
      snapshot,
      actorIndex,
      pendingEvents,
      eventDigests: new Map(pendingValues.map((event) => [event.eventId, createHash('sha256').update(canonicalJson(event)).digest('hex')])),
      generation: checkpoint.generation,
      nextSequence,
      ackedSequence,
      closed: last.type === 'meeting.ended' || last.type === 'meeting.aborted' || last.type === 'recording.authoritative_ready',
      lastOccurredAt: last.occurredAt,
      lastAcknowledgedEventId: ackedSequence < 0 ? null : cursor.eventId,
      lastAcknowledgedDigest: ackedSequence < 0 ? null : cursor.digestSha256,
      maintenanceNeeded: ackedSequence >= 128
    };
  }

  private persistLifecycleV3Snapshot(snapshot: DurableCraigLifecycleV3Snapshot): void {
    if (this.pendingLifecycleV3Root === undefined) throw new Error('Lifecycle v3 durable outbox is not configured');
    const normalized = restoreCraigLifecycleV3ProducerFromSnapshot(snapshot).durableSnapshot();
    assertRecordingId(normalized.recordingId);
    mkdirSync(this.pendingLifecycleV3Root, { recursive: true, mode: 0o700 });
    const journal = this.readLifecycleV3Journal(normalized.recordingId);
    const existing = new Set(journal?.eventDigests.keys() ?? []);
    let sequence = journal?.nextSequence ?? 0;
    for (const event of normalized.pendingOutbox)
      if (!existing.has(event.eventId)) this.appendLifecycleV3Event(normalized, event, sequence++);
  }

  private readLifecycleV3Snapshot(recordingId: string): DurableCraigLifecycleV3Snapshot | undefined {
    if (this.pendingLifecycleV3Root === undefined) return undefined;
    assertRecordingId(recordingId);
    const journal = this.readLifecycleV3Journal(recordingId);
    if (journal !== undefined) {
      const pendingEvents = [...journal.pendingEvents.values()];
      const pinnedStart = pendingEvents.find(({ type }) => type === 'meeting.started') ??
        journal.snapshot.pendingOutbox.find(({ type }) => type === 'meeting.started');
      const events = pinnedStart === undefined
        ? pendingEvents
        : [pinnedStart, ...pendingEvents.filter(({ eventId }) => eventId !== pinnedStart.eventId)];
      return restoreCraigLifecycleV3ProducerFromSnapshot({
        ...journal.snapshot,
        emitted: events.map(({ eventId, occurredAt }) => ({ eventId, occurredAt })),
        pendingOutbox: events
      }).durableSnapshot();
    }
    const filePath = path.join(this.pendingLifecycleV3Root, `${recordingId}.json`);
    if (!existsSync(filePath)) return undefined;
    return restoreCraigLifecycleV3ProducerFromSnapshot(JSON.parse(readFileSync(filePath, 'utf8')) as unknown).durableSnapshot();
  }

  private restoreLifecycleV3Admissions(): void {
    if (this.pendingLifecycleV3Root === undefined || this.rejectedLifecycleV3Root === undefined) return;
    mkdirSync(this.pendingLifecycleV3Root, { recursive: true, mode: 0o700 });
    mkdirSync(this.rejectedLifecycleV3Root, { recursive: true, mode: 0o700 });
    for (const entry of readdirSync(this.pendingLifecycleV3Root)
      .filter((name) => name.endsWith('.json') || name.endsWith('.journal'))
      .sort()) {
      const filePath = path.join(this.pendingLifecycleV3Root, entry);
      try {
        const recordingId = entry.endsWith('.journal') ? entry.slice(0, -'.journal'.length) : entry.slice(0, -'.json'.length);
        const snapshot = this.readLifecycleV3Snapshot(recordingId)!;
        const journal = entry.endsWith('.journal') ? this.readLifecycleV3Journal(recordingId) : undefined;
        if (entry !== `${snapshot.recordingId}.json` && entry !== `${snapshot.recordingId}.journal`)
          throw new Error('Lifecycle v3 durable snapshot filename does not match its recording');
        const queuedLifecycleEvents = this.queueLength() - this.queuedPackets;
        if (queuedLifecycleEvents + snapshot.pendingOutbox.length > this.maxQueuedLifecycleEvents) {
          this.logger.warn(
            `Lifecycle v3 recovery capacity is exhausted; leaving durable snapshot ${entry} pending for a later restart`
          );
          continue;
        }
        const restoredPending = journal === undefined ? snapshot.pendingOutbox : [...journal.pendingEvents.values()];
        for (const event of restoredPending) {
          if (!this.queue.slice(this.queueHead).some((item) => item.type === 'lifecycle' && item.event.eventId === event.eventId))
            this.queue.push({ type: 'lifecycle', event });
          if (event.type === 'meeting.started') this.openRecordings.add(event.recordingId);
          if (event.type === 'meeting.ended' || event.type === 'meeting.aborted') {
            this.openRecordings.delete(event.recordingId);
            this.rememberClosedRecording(event.recordingId);
          }
        }
        if (journal !== undefined && snapshot.pendingOutbox.every((event) => event.type !== 'meeting.started') && !journal.closed)
          this.openRecordings.add(recordingId);
      } catch (error) {
        this.logger.error(`Rejecting invalid lifecycle v3 durable snapshot ${entry}`, error);
        renameSync(filePath, path.join(this.rejectedLifecycleV3Root, entry));
      }
    }
  }

  private removeLifecycleV3Snapshot(recordingId: string): void {
    if (this.pendingLifecycleV3Root === undefined) return;
    try {
      const legacy = path.join(this.pendingLifecycleV3Root, `${recordingId}.json`);
      if (existsSync(legacy)) unlinkSync(legacy);
      const journal = this.lifecycleV3JournalRoot(recordingId);
      if (existsSync(journal)) {
        for (const entry of readdirSync(journal)) unlinkSync(path.join(journal, entry));
        rmdirSync(journal);
      }
      syncDirectorySync(this.pendingLifecycleV3Root);
      this.lifecycleV3JournalIndex.delete(recordingId);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT')
        this.logger.warn(`Failed to remove delivered lifecycle v3 snapshot for ${recordingId}: ${String(error)}`);
    }
  }

  private acknowledgeLifecycleV3Event(event: CraigLifecycleV3Event): void {
    const root = this.lifecycleV3JournalRoot(event.recordingId);
    if (!existsSync(root)) return;
    const indexed = this.readLifecycleV3Journal(event.recordingId);
    if (indexed === undefined) throw new Error('Lifecycle v3 acknowledgement journal is missing');
    const currentSequence = indexed.ackedSequence;
    const eventDigest = createHash('sha256').update(canonicalJson(event)).digest('hex');
    if (indexed.lastAcknowledgedEventId === event.eventId) {
      if (indexed.lastAcknowledgedDigest !== eventDigest)
        throw new Error('Lifecycle v3 duplicate acknowledgement conflicts with its durable cursor');
      return;
    }
    const sequence = currentSequence + 1;
    const pending = indexed.pendingEvents.get(sequence);
    if (pending?.eventId !== event.eventId || indexed.eventDigests.get(event.eventId) !== eventDigest)
      throw new Error('Lifecycle v3 acknowledgement conflicts with its durable segment');
    this.writeDurableJson(path.join(root, 'cursor.json'), {
      schemaVersion: 1,
      generation: indexed.generation,
      ackedSequence: sequence,
      eventId: event.eventId,
      digestSha256: eventDigest
    });
    this.reconcileLifecycleV3AcknowledgementMemory(event, sequence);
  }

  private lifecycleV3MaintenancePath(root: string): string { return path.join(root, 'maintenance.json'); }

  private readLifecycleV3Maintenance(root: string): LifecycleV3MaintenanceState | undefined {
    try { return JSON.parse(readFileSync(this.lifecycleV3MaintenancePath(root), 'utf8')) as LifecycleV3MaintenanceState; }
    catch { return undefined; }
  }

  /** A chunk is immutable and contains no more than K logical actor/event records. */
  private writeLifecycleV3MaintenanceChunk(root: string, state: LifecycleV3MaintenanceState, records: unknown[]): void {
    if (records.length === 0 || records.length > lifecycleV3MaintenanceRecordsPerStep)
      throw new Error('Lifecycle v3 maintenance chunk exceeds its fixed record budget');
    const file = `generation-${String(state.targetGeneration).padStart(8, '0')}-chunk-${String(state.chunkCount).padStart(8, '0')}.json`;
    const payload = { schemaVersion: 1, generation: state.targetGeneration, index: state.chunkCount, records };
    const checksum = createHash('sha256').update(canonicalJson(payload)).digest('hex');
    const chunkPath = path.join(root, file);
    const durable = { ...payload, checksumSha256: checksum };
    if (existsSync(chunkPath)) {
      if (canonicalJson(JSON.parse(readFileSync(chunkPath, 'utf8'))) !== canonicalJson(durable))
        throw new Error('Lifecycle v3 immutable maintenance chunk conflicts with persisted state');
    } else this.writeDurableJson(chunkPath, durable);
    state.chunksChecksumSha256 = createHash('sha256').update(`${state.chunksChecksumSha256}:${checksum}`).digest('hex');
    state.chunkCount++;
  }

  private startLifecycleV3Maintenance(root: string, journal: LifecycleV3JournalState): LifecycleV3MaintenanceState {
    const manifest = this.tryReadLifecycleV3Manifest(root);
    if (manifest === undefined) throw new Error('Lifecycle v3 maintenance manifest is missing');
    const base = this.readLifecycleV3Generation(root, manifest.current);
    let obsoleteChunkCount = 0;
    if (manifest.previous !== null) {
      try {
        const obsolete = JSON.parse(readFileSync(path.join(root, manifest.previous.file), 'utf8')) as any;
        obsoleteChunkCount = obsolete.schemaVersion === 4 && Number.isSafeInteger(obsolete.chunkCount) ? obsolete.chunkCount : 0;
      } catch { /* recovery will retain or reject this already-invalid fallback */ }
    }
    const state: LifecycleV3MaintenanceState = {
      schemaVersion: 1, phase: 'capture-base', sourceGeneration: manifest.current.generation,
      targetGeneration: manifest.current.generation + 1, targetSequence: journal.ackedSequence,
      baseActorCursor: 0, deltaCursor: (base.baseSequence ?? 0) + 1, deltaActorCursor: 0,
      chunkCount: 0, chunksChecksumSha256: '', cleanupCursor: 1,
      previousCoveredDeltaCursor: manifest.previous?.coveredDeltaCursor ?? 0,
      obsoleteGeneration: manifest.previous?.generation ?? null, obsoleteChunkCount
    };
    this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state);
    return state;
  }

  private runLifecycleV3MaintenanceJournalStep(recordingId: string, journal: LifecycleV3JournalState): boolean {
    const root = this.lifecycleV3JournalRoot(recordingId);
    const state = this.readLifecycleV3Maintenance(root) ?? this.startLifecycleV3Maintenance(root, journal);
    const manifest = this.tryReadLifecycleV3Manifest(root);
    if (manifest === undefined) throw new Error('Lifecycle v3 maintenance manifest is missing');

    if (state.phase === 'capture-base') {
      const base = this.readLifecycleV3Generation(root, manifest.current.generation === state.sourceGeneration
        ? manifest.current : manifest.previous!);
      const actors = (base.actors ?? []) as unknown[];
      const records: unknown[] = [];
      if (state.baseActorCursor === 0) records.push({ kind: 'base', value: { ...base, actors: [], sealedReady: base.sealedReady === null ? null : { ...base.sealedReady, actors: [] } } });
      while (records.length < lifecycleV3MaintenanceRecordsPerStep && state.baseActorCursor < actors.length)
        records.push({ kind: 'actor', value: actors[state.baseActorCursor++] });
      if (records.length > 0) this.writeLifecycleV3MaintenanceChunk(root, state, records);
      if (state.baseActorCursor >= actors.length) state.phase = 'capture-deltas';
      this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state);
      return true;
    }

    if (state.phase === 'capture-deltas') {
      if (state.deltaCursor > state.targetSequence) state.phase = 'publish';
      else {
        const deltaPath = path.join(root, `${String(state.deltaCursor).padStart(8, '0')}.event.json`);
        if (!existsSync(deltaPath)) throw new Error('Lifecycle v3 maintenance delta is missing');
        const delta = JSON.parse(readFileSync(deltaPath, 'utf8')) as any;
        const actors: unknown[] = delta.type === 'recording.authoritative_ready' || delta.type === 'meeting.started'
          ? delta.actors : delta.type === 'participant.joined' ? [delta.actor] : [];
        const records: unknown[] = [];
        if (state.deltaActorCursor === 0) {
          let value = delta;
          if (delta.type === 'recording.authoritative_ready' || delta.type === 'meeting.started')
            value = { ...delta, actors: [] };
          else if (delta.type === 'participant.joined') {
            const { actor: _capturedSeparately, ...eventWithoutActor } = delta;
            value = eventWithoutActor;
          }
          records.push({ kind: 'event', sequence: state.deltaCursor, value });
        }
        while (records.length < lifecycleV3MaintenanceRecordsPerStep && state.deltaActorCursor < actors.length)
          records.push({ kind: 'event-actor', sequence: state.deltaCursor, value: actors[state.deltaActorCursor++] });
        this.writeLifecycleV3MaintenanceChunk(root, state, records);
        if (state.deltaActorCursor >= actors.length) { state.deltaCursor++; state.deltaActorCursor = 0; }
      }
      this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state);
      return true;
    }

    if (state.phase === 'publish') {
      if (manifest.current.generation === state.targetGeneration) {
        journal.generation = state.targetGeneration;
        state.phase = 'cleanup-deltas';
        this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state);
        return true;
      }
      const descriptor = {
        schemaVersion: 4, generation: state.targetGeneration, coveredDeltaCursor: state.targetSequence,
        chunkCount: state.chunkCount, chunksChecksumSha256: state.chunksChecksumSha256
      };
      const checksumSha256 = createHash('sha256').update(canonicalJson(descriptor)).digest('hex');
      const file = `snapshot-${String(state.targetGeneration).padStart(8, '0')}.json`;
      this.writeDurableJson(path.join(root, file), { ...descriptor, checksumSha256 });
      // The only publication point. Until this rename-backed write, recovery can
      // select only the old complete generation; appended deltas remain untouched.
      const current = { generation: state.targetGeneration, file, checksumSha256, coveredDeltaCursor: state.targetSequence };
      this.writeDurableJson(path.join(root, 'manifest.previous.json'), manifest);
      this.writeDurableJson(path.join(root, 'manifest.json'), { schemaVersion: 1, current, previous: manifest.current });
      journal.generation = state.targetGeneration;
      state.previousCoveredDeltaCursor = manifest.current.coveredDeltaCursor;
      state.phase = 'cleanup-deltas';
      this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state);
      return true;
    }

    if (state.phase === 'cleanup-deltas') {
      let work = 0;
      while (work++ < lifecycleV3MaintenanceRecordsPerStep && state.cleanupCursor < state.previousCoveredDeltaCursor) {
        const oldPath = path.join(root, `${String(state.cleanupCursor++).padStart(8, '0')}.event.json`);
        if (existsSync(oldPath)) unlinkSync(oldPath);
      }
      if (state.cleanupCursor >= state.previousCoveredDeltaCursor) { state.phase = 'cleanup-generations'; state.cleanupCursor = 0; }
      this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state);
      return true;
    }

    // Cleanup is deliberately one generation/chunk per call. Published files are
    // never modified, and current+previous always remain available.
    if (state.obsoleteGeneration !== null) {
      if (state.cleanupCursor < state.obsoleteChunkCount) {
        const chunk = path.join(root, `generation-${String(state.obsoleteGeneration).padStart(8, '0')}-chunk-${String(state.cleanupCursor++).padStart(8, '0')}.json`);
        if (existsSync(chunk)) unlinkSync(chunk);
        this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state);
        return true;
      }
      const snapshot = path.join(root, `snapshot-${String(state.obsoleteGeneration).padStart(8, '0')}.json`);
      if (existsSync(snapshot)) { unlinkSync(snapshot); state.obsoleteGeneration = null; this.writeDurableJson(this.lifecycleV3MaintenancePath(root), state); return true; }
      state.obsoleteGeneration = null;
    }
    unlinkSync(this.lifecycleV3MaintenancePath(root));
    journal.maintenanceNeeded = journal.ackedSequence >= state.targetSequence + 128;
    return journal.maintenanceNeeded;
  }

  private reconcileLifecycleV3AcknowledgementMemory(event: CraigLifecycleV3Event, sequence: number): void {
    const indexed = this.lifecycleV3JournalIndex.get(event.recordingId);
    if (!indexed || indexed.ackedSequence >= sequence) return;
    const pending = indexed.pendingEvents.get(sequence);
    if (pending?.eventId !== event.eventId || canonicalJson(pending) !== canonicalJson(event))
      throw new Error('Lifecycle v3 in-memory acknowledgement cursor is inconsistent');
    indexed.ackedSequence = sequence;
    indexed.pendingEvents.delete(sequence);
    indexed.eventDigests.delete(event.eventId);
    indexed.lastAcknowledgedEventId = event.eventId;
    indexed.lastAcknowledgedDigest = createHash('sha256').update(canonicalJson(event)).digest('hex');
    if (sequence >= 128 && sequence % 128 === 0) {
      indexed.maintenanceNeeded = true;
      this.lifecycleV3MaintenanceQueue.add(event.recordingId);
      this.scheduleLifecycleV3Maintenance();
    }
  }

  /**
   * Explicit off-delivery maintenance port. ACK only flips a bit; the owning
   * composition drives this port from its idle/maintenance scheduler.
   * Returns true when another journal still needs a step.
   */
  runLifecycleV3MaintenanceStep(): boolean {
    const recordingId = this.lifecycleV3MaintenanceQueue.values().next().value as string | undefined;
    if (recordingId === undefined) return false;
    const journal = this.lifecycleV3JournalIndex.get(recordingId);
    if (journal === undefined) { this.lifecycleV3MaintenanceQueue.delete(recordingId); return this.lifecycleV3MaintenanceQueue.size > 0; }
    const again = this.runLifecycleV3MaintenanceJournalStep(recordingId, journal);
    this.lifecycleV3MaintenanceQueue.delete(recordingId);
    if (again) this.lifecycleV3MaintenanceQueue.add(recordingId);
    return this.lifecycleV3MaintenanceQueue.size > 0;
  }

  private scheduleLifecycleV3Maintenance(delayMs = 0): void {
    if (this.lifecycleV3MaintenanceTimer || this.lifecycleV3MaintenanceQueue.size === 0) return;
    this.lifecycleV3MaintenanceTimer = setTimeout(() => {
      this.lifecycleV3MaintenanceTimer = null;
      try {
        const more = this.runLifecycleV3MaintenanceStep();
        this.lifecycleV3MaintenanceFailures = 0;
        if (more) this.scheduleLifecycleV3Maintenance();
      } catch (error) {
        this.lifecycleV3MaintenanceFailures++;
        const retryMs = retryDelay(this.lifecycleV3MaintenanceFailures);
        this.logger.error(`Lifecycle v3 maintenance failed; retrying in ${retryMs}ms`, error);
        this.scheduleLifecycleV3Maintenance(retryMs);
      }
      this.notifyIfDrained();
    }, delayMs);
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

  private queueLength(): number {
    return this.queue.length - this.queueHead;
  }

  /** Head-index dequeue; occasional bulk copy keeps the amortized cost O(1). */
  private advanceQueue(count: number): void {
    this.queueHead += count;
    if (this.queueHead === this.queue.length) {
      this.queue = [];
      this.queueHead = 0;
    } else if (this.queueHead >= 1024 && this.queueHead * 2 >= this.queue.length) {
      this.queue = this.queue.slice(this.queueHead);
      this.queueHead = 0;
    }
  }

  private scheduleProcessing(delayMs = 0) {
    if (this.processing || this.retryTimer || this.queueLength() === 0) return;
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
    if (this.processing || this.queueLength() === 0) return;
    this.processing = true;

    try {
      const first = this.queue[this.queueHead];
      if (first.type === 'lifecycle') {
        await this.transport.post('/v1/craig/events', first.event);
        if (first.event.schemaVersion === 3) this.acknowledgeLifecycleV3Event(first.event);
        this.advanceQueue(1);
      } else {
        const batch: WireVoicePacket[] = [];
        for (let index = this.queueHead; index < this.queue.length; index++) {
          const item = this.queue[index];
          if (item.type !== 'voice' || batch.length >= this.batchSize) break;
          batch.push(item.packet);
        }
        await this.transport.post('/v1/craig/voice-packets', {
          schemaVersion: 1,
          packets: batch
        });
        this.advanceQueue(batch.length);
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

      const first = this.queue[this.queueHead];
      if (first.type === 'lifecycle') this.advanceQueue(1);
      else {
        let discardedPackets = 0;
        for (let index = this.queueHead; index < this.queue.length; index++) {
          const item = this.queue[index];
          if (item.type !== 'voice' || discardedPackets >= this.batchSize) break;
          discardedPackets++;
        }
        this.advanceQueue(discardedPackets);
        this.queuedPackets -= discardedPackets;
      }
      this.consecutiveFailures = 0;
      this.logger.error('Meeting integration delivery was permanently rejected; discarding it so FIFO can continue', error);
    }

    this.processing = false;
    if (this.queueLength() > 0) this.scheduleProcessing();
    else this.scheduleOriginalProcessing();
    this.notifyIfDrained();
  }

  private scheduleOriginalProcessing(delayMs = 0): void {
    if (
      this.processingOriginal ||
      this.originalRetryTimer ||
      this.originalJobs.length === 0 ||
      this.processing ||
      this.queueLength() > 0 ||
      this.originalCooker === undefined ||
      this.transport.postAuthoritativeTrack === undefined ||
      this.transport.postAuthoritativeReady === undefined
    )
      return;
    this.originalRetryTimer = setTimeout(() => {
      this.originalRetryTimer = null;
      if (this.processing || this.queueLength() > 0) return;
      void this.processOriginal();
    }, delayMs);
  }

  private async processOriginal(): Promise<void> {
    if (this.processingOriginal || this.originalJobs.length === 0 || this.processing || this.queueLength() > 0) return;
    this.processingOriginal = true;
    const pending = this.originalJobs[0];

    try {
      pending.job = await this.prepareOriginalJob(pending);
      await this.deliverOriginalJob(pending.job);
      await unlink(pending.filePath);
      this.removeLifecycleV3Snapshot(pending.job.recordingId);
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
      pending.job.sourceFiles.every((source) => source.checksumSha256 !== undefined && source.sizeBytes !== undefined) &&
      (pending.job.lifecycleV3Snapshot === undefined || pending.job.lifecycleV3Snapshot.sealedReady !== null)
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
    let prepared: OriginalRecordingOutboxJob = {
      ...pending.job,
      sourceFiles,
      authoritativeTracks: users.tracks.map((track) => ({
        ...track,
        timelineOffsetMs: data.timelineOffsetMs
      })),
      authoritativeTimelineBasis
    };
    if (prepared.lifecycleV3Snapshot !== undefined) {
      const lifecycle = restoreCraigLifecycleV3ProducerFromSnapshot(prepared.lifecycleV3Snapshot);
      lifecycle.authoritativeReady(
        {
          eventId: `${prepared.recordingId}:authoritative-ready:v3`,
          recordingId: prepared.recordingId,
          guildId: prepared.guildId,
          channelId: prepared.channelId,
          occurredAt: prepared.terminalEvent.occurredAt
        },
        {
          actors: prepared.authoritativeTracks!.map(({ speakerId }) => ({ id: speakerId })),
          endedAt: prepared.terminalEvent.occurredAt,
          trackCount: prepared.authoritativeTracks!.length,
          sourceFilesChecksumSha256: sourceFilesChecksum(prepared.sourceFiles)
        }
      );
      prepared = { ...prepared, lifecycleV3Snapshot: lifecycle.durableSnapshot() };
    }
    await writeOriginalRecordingJob(pending.filePath, prepared);
    return prepared;
  }

  private async deliverOriginalJob(job: OriginalRecordingOutboxJob): Promise<void> {
    if (job.lifecycleV3Snapshot === undefined) {
      await this.transport.post('/v1/craig/events', job.startedEvent);
      await this.transport.post('/v1/craig/events', job.terminalEvent);
    } else {
      const durableEvents = restoreCraigLifecycleV3ProducerFromSnapshot(job.lifecycleV3Snapshot).durableSnapshot().pendingOutbox;
      for (const event of durableEvents) if (event.type !== 'recording.authoritative_ready') await this.transport.post('/v1/craig/events', event);
    }
    if (job.terminalEvent.type === 'meeting.aborted') return;

    if (job.authoritativeTracks === undefined) throw new PermanentOriginalRecordingError('Original recording track metadata was not prepared');
    for (const track of job.authoritativeTracks) {
      const cooked = await this.originalCooker!.cook(job.recordingId, track.trackNumber);
      try {
        const localIdentity = await checksumFile(cooked.filePath);
        if (localIdentity.checksumSha256 !== cooked.checksumSha256 || localIdentity.sizeBytes !== cooked.sizeBytes)
          throw new Error('Cooked authoritative track identity does not match its exact local bytes');
        const uploadMetadata: AuthoritativeTrackMetadata = {
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
          };
        const uploadAcknowledgement = parseDurableTrackUploadAcknowledgement(
          await this.transport.postAuthoritativeTrack!(
          uploadMetadata,
          cooked.filePath
          ),
          uploadMetadata
        );
        await this.finalizeCancellationProofsAfterUpload(job, track, uploadAcknowledgement);
      } finally {
        await cooked.dispose().catch((error) => this.logger.warn(`Failed to remove cooked track ${cooked.filePath}: ${String(error)}`));
      }
    }

    if (job.lifecycleV3Snapshot !== undefined) {
      const ready = restoreCraigLifecycleV3ProducerFromSnapshot(job.lifecycleV3Snapshot).durableSnapshot().sealedReady;
      if (ready === null || ready.type !== 'recording.authoritative_ready')
        throw new PermanentOriginalRecordingError('Lifecycle v3 outbox was not sealed before authoritative publication');
      await this.transport.postAuthoritativeReady!(ready);
    } else
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

  private async finalizeCancellationProofsAfterUpload(
    job: OriginalRecordingOutboxJob,
    track: PreparedAuthoritativeTrack,
    uploadAcknowledgement: DurableAuthoritativeTrackUploadAcknowledgement
  ): Promise<void> {
    if (this.transport.postCancellationPcmFence === undefined) return;
    const info = JSON.parse(readFileSync(path.join(this.recordingRoot!, `${job.recordingId}.ogg.info`), 'utf8')) as { clientId?: unknown };
    if (info.clientId !== track.speakerId) return;
    const prefix = `${job.recordingId}.ogg.playback-cancellation-fence.`;
    for (const entry of readdirSync(this.recordingRoot!).filter((name) => name.startsWith(prefix) && name.endsWith('.json')).sort()) {
      const sidecar = JSON.parse(readFileSync(path.join(this.recordingRoot!, entry), 'utf8')) as any;
      // Meeting v1 omitted observedAt. Such records remain durable, but cannot be promoted by inventing verifier fields.
      if (
        sidecar.schemaVersion !== 2 ||
        sidecar.recordingId !== job.recordingId ||
        typeof sidecar.meetingId !== 'string' ||
        !Number.isSafeInteger(sidecar.cancellationObservedAtMs) || sidecar.cancellationObservedAtMs < 0 ||
        typeof sidecar.reason !== 'string' || sidecar.reason.length === 0
      ) continue;
      const observedAt = dateMillisecondsToIsoOrThrow(sidecar.cancellationObservedAtMs, 'cancellationObservedAtMs');
      const fenceObservedAt = Number.isSafeInteger(sidecar.fenceObservedAtMs) && sidecar.fenceObservedAtMs >= sidecar.cancellationObservedAtMs
        ? dateMillisecondsToIsoOrThrow(sidecar.fenceObservedAtMs, 'fenceObservedAtMs')
        : observedAt;
      const acceptedPacketCountAfterCancellation = sidecar.postCancellationAcceptedPacketCount;
      const attemptedPacketCountAfterCancellation = sidecar.postCancellationAttemptedPacketCount;
      if (!Number.isSafeInteger(acceptedPacketCountAfterCancellation) || acceptedPacketCountAfterCancellation < 0 ||
          !Number.isSafeInteger(attemptedPacketCountAfterCancellation) || attemptedPacketCountAfterCancellation < acceptedPacketCountAfterCancellation)
        throw new Error('Durable cancellation attempt counters are malformed');
      const proof = createAuthoritativeCancellationPcmFenceLog({
        attemptedPacketCountAfterCancellation,
        attemptId: sidecar.attemptId,
        cancellationObservedAt: observedAt,
        fenceObservedAt,
        meetingId: sidecar.meetingId,
        recordingId: job.recordingId,
        trackSha256: uploadAcknowledgement.checksumSha256,
        turnId: sidecar.turnId,
        acceptedPacketCountAfterCancellation
      });
      const proofRoot = path.join(this.pendingOriginalRoot!, 'cancellation-pcm-fence');
      const manifestRoot = path.join(this.pendingOriginalRoot!, 'cancellation-pcm-fence-manifests');
      mkdirSync(proofRoot, { recursive: true, mode: 0o700 });
      mkdirSync(manifestRoot, { recursive: true, mode: 0o700 });
      const proofKey = createHash('sha256').update(`${proof.meetingId}\0${proof.turnId}\0${proof.attemptId}`).digest('hex').slice(0, 32);
      const proofPath = path.join(proofRoot, `${job.recordingId}.pcm-fence.${proofKey}.json`);
      const manifestPath = path.join(manifestRoot, `${job.recordingId}.pcm-fence.${proofKey}.json`);
      const manifest = {
        schemaVersion: 1,
        uploadAcknowledgement,
        uploadId: `authoritative-track:v1:${job.recordingId}:${track.trackNumber}`,
        recordingId: job.recordingId,
        guildId: job.guildId,
        channelId: job.channelId,
        speakerId: track.speakerId,
        trackNumber: track.trackNumber,
        trackSha256: uploadAcknowledgement.checksumSha256,
        trackSizeBytes: uploadAcknowledgement.sizeBytes,
        proof
      };
      if (existsSync(manifestPath)) {
        if (canonicalJson(JSON.parse(readFileSync(manifestPath, 'utf8'))) !== canonicalJson(manifest))
          throw new Error('Conflicting immutable uploaded Botik track manifest');
      } else this.writeDurableJson(manifestPath, manifest);
      if (existsSync(proofPath)) {
        const persisted = JSON.parse(readFileSync(proofPath, 'utf8'));
        if (canonicalJson(persisted) !== canonicalJson(proof)) throw new Error('Conflicting durable cancellation PCM proof');
      } else this.writeDurableJson(proofPath, proof);
      this.enqueueCancellationProof(proofPath);
      this.scheduleCancellationProofProcessing();
    }
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
    return this.queueLength() === 0 && !this.processing && this.originalJobs.length === 0 && !this.processingOriginal &&
      this.lifecycleV3MaintenanceQueue.size === 0 && this.lifecycleV3MaintenanceTimer === null &&
      this.cancellationProofJobs.length === 0 && !this.processingCancellationProof && this.cancellationProofRetryTimer === null;
  }

  private notifyIfDrained(): void {
    if (!this.isDrained()) return;
    for (const waiter of this.drainWaiters) waiter();
    this.drainWaiters.clear();
  }
}

export class HttpMeetingIntegrationTransport implements MeetingIntegrationTransport, MeetingPlatformConfigurationClient {
  constructor(private readonly endpoint: URL, private readonly token: string, private readonly requestTimeoutMs: number) {}

  async getConfiguration(): Promise<MeetingPlatformConfiguration> {
    const response = await this.request(
      '/v1/craig/configuration',
      {
        method: 'GET',
        headers: {
          authorization: `Bearer ${this.token}`
        }
      },
      200
    );
    return parseMeetingPlatformConfiguration(await response.json());
  }

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

  async postAuthoritativeTrack(metadata: AuthoritativeTrackMetadata, audioFilePath: string): Promise<DurableAuthoritativeTrackUploadAcknowledgement> {
    const stream = createReadStream(audioFilePath);
    try {
      const response = await this.request('/v1/craig/authoritative-tracks', {
        method: 'POST',
        headers: {
          authorization: `Bearer ${this.token}`,
          'content-length': String(metadata.sizeBytes),
          'content-type': 'audio/ogg',
          'x-craig-authoritative-track-metadata': Buffer.from(JSON.stringify(metadata), 'utf8').toString('base64url')
        },
        body: stream as any
      });
      return parseDurableTrackUploadAcknowledgement(await response.json(), metadata);
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

  async postCancellationPcmFence(proof: CraigAuthoritativeCancellationPcmFenceLog): Promise<CancellationPcmFenceProofReceipt> {
    const response = await this.request('/v1/craig/events', {
      method: 'POST', headers: { authorization: `Bearer ${this.token}`, 'content-type': 'application/json' }, body: JSON.stringify(proof)
    });
    return await response.json() as CancellationPcmFenceProofReceipt;
  }

  private async request(pathname: string, init: Parameters<typeof fetch>[1], expectedStatus?: number): Promise<Response> {
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
      return response;
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function checksumFile(filePath: string): Promise<{ checksumSha256: string; sizeBytes: number }> {
  const checksum = createHash('sha256');
  let sizeBytes = 0;
  for await (const rawChunk of createReadStream(filePath)) {
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk as Uint8Array);
    checksum.update(chunk);
    sizeBytes += chunk.byteLength;
  }
  return { checksumSha256: checksum.digest('hex'), sizeBytes };
}

function parseDurableTrackUploadAcknowledgement(
  value: unknown,
  expected: AuthoritativeTrackMetadata
): DurableAuthoritativeTrackUploadAcknowledgement {
  if (!isRecord(value) || Object.keys(value).sort().join(',') !==
    'checksumSha256,durable,immutable,object,recordingId,schemaVersion,sizeBytes,trackNumber,uploadId')
    throw new Error('Authoritative track upload acknowledgement is malformed');
  const object = value.object;
  if (!isRecord(object) || Object.keys(object).sort().join(',') !== 'bucket,key,provider,versionId' ||
      object.provider !== 's3' || typeof object.bucket !== 'string' || object.bucket.length === 0 ||
      typeof object.key !== 'string' || object.key.length === 0 || typeof object.versionId !== 'string' || object.versionId.length === 0)
    throw new Error('Authoritative track upload acknowledgement lacks immutable S3 identity');
  if (value.schemaVersion !== 1 || value.durable !== true || value.immutable !== true ||
      value.uploadId !== expected.uploadId || value.recordingId !== expected.recordingId ||
      value.trackNumber !== expected.trackNumber || value.checksumSha256 !== expected.checksumSha256 ||
      value.sizeBytes !== expected.sizeBytes)
    throw new Error('Authoritative track upload acknowledgement does not match uploaded bytes');
  return value as unknown as DurableAuthoritativeTrackUploadAcknowledgement;
}

function cancellationProofId(proof: CraigAuthoritativeCancellationPcmFenceLog): string {
  return createHash('sha256').update(canonicalJson(proof)).digest('hex');
}

function assertCancellationProofReceipt(
  value: unknown,
  proof: CraigAuthoritativeCancellationPcmFenceLog,
  upload: DurableAuthoritativeTrackUploadAcknowledgement
): asserts value is CancellationPcmFenceProofReceipt {
  if (!isRecord(value) || Object.keys(value).sort().join(',') !== 'checksumSha256,object,proofId,schemaVersion,sizeBytes')
    throw new Error('Cancellation PCM proof receipt is malformed');
  if (value.schemaVersion !== 1 || value.proofId !== cancellationProofId(proof) ||
      value.sizeBytes !== upload.sizeBytes || value.checksumSha256 !== upload.checksumSha256 ||
      canonicalJson(value.object) !== canonicalJson(upload.object))
    throw new Error('Cancellation PCM proof receipt does not match its immutable upload manifest');
}

function readCancellationProofManifest(
  manifestPath: string,
  proof: CraigAuthoritativeCancellationPcmFenceLog
): { uploadAcknowledgement: DurableAuthoritativeTrackUploadAcknowledgement } {
  if (!existsSync(manifestPath)) throw new Error('Cancellation PCM proof manifest is missing');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as Record<string, unknown>;
  if (Object.keys(manifest).sort().join(',') !==
      'channelId,guildId,proof,recordingId,schemaVersion,speakerId,trackNumber,trackSha256,trackSizeBytes,uploadAcknowledgement,uploadId' ||
      manifest.schemaVersion !== 1 || manifest.recordingId !== proof.recordingId ||
      typeof manifest.guildId !== 'string' || !discordSnowflake.test(manifest.guildId) ||
      typeof manifest.channelId !== 'string' || !discordSnowflake.test(manifest.channelId) ||
      typeof manifest.speakerId !== 'string' || !discordSnowflake.test(manifest.speakerId) ||
      !Number.isSafeInteger(manifest.trackNumber) || Number(manifest.trackNumber) < 1 ||
      manifest.trackSha256 !== proof.trackSha256 || typeof manifest.trackSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/.test(manifest.trackSha256) ||
      !Number.isSafeInteger(manifest.trackSizeBytes) || Number(manifest.trackSizeBytes) < 1 ||
      manifest.uploadId !== `authoritative-track:v1:${proof.recordingId}:${String(manifest.trackNumber)}` ||
      canonicalJson(manifest.proof) !== canonicalJson(proof) || !isRecord(manifest.uploadAcknowledgement))
    throw new Error('Cancellation PCM proof manifest is corrupt or mismatched');
  const upload = manifest.uploadAcknowledgement as unknown as DurableAuthoritativeTrackUploadAcknowledgement;
  parseDurableTrackUploadAcknowledgement(upload, {
    schemaVersion: 1, uploadId: String(manifest.uploadId), recordingId: proof.recordingId,
    trackNumber: Number(manifest.trackNumber), guildId: manifest.guildId, channelId: manifest.channelId,
    speakerId: manifest.speakerId, timelineOffsetMs: 0, checksumSha256: manifest.trackSha256,
    sizeBytes: Number(manifest.trackSizeBytes)
  });
  return { uploadAcknowledgement: upload };
}

export async function createMeetingIntegrationSink(
  config: MeetingIntegrationConfig | undefined,
  logger: MeetingIntegrationLogger,
  recordingRoot?: string
): Promise<MeetingIntegrationSink> {
  if (!config?.enabled) return new NoopMeetingIntegrationSink();
  const transport = await createHttpMeetingIntegrationTransport(config, logger);
  if (!transport) return new NoopMeetingIntegrationSink();

  const sink = new BoundedMeetingIntegrationSink(
    transport,
    logger,
    config.maxQueuedPackets,
    config.batchSize,
    1024,
    recordingRoot === undefined ? undefined : { recordingRoot },
    config.lifecycleProducer ?? { schemaVersion: 1 }
  );
  await sink.restoreOriginalRecordingJobs();
  return sink;
}

export async function createMeetingPlatformConfigurationClient(
  config: MeetingIntegrationConfig | undefined,
  logger: MeetingIntegrationLogger
): Promise<MeetingPlatformConfigurationClient | undefined> {
  return await createHttpMeetingIntegrationTransport(config, logger);
}

async function createHttpMeetingIntegrationTransport(
  config: MeetingIntegrationConfig | undefined,
  logger: MeetingIntegrationLogger
): Promise<HttpMeetingIntegrationTransport | undefined> {
  if (!config?.enabled) return undefined;

  const endpoint = new URL(config.endpoint);
  if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password)
    throw new Error('Meeting integration endpoint must be an HTTP(S) URL without embedded credentials');

  const token = (await readFile(config.tokenFile, 'utf8')).trim();
  if (!token) throw new Error('Meeting integration token file is empty');

  const requestTimeoutMs = config.requestTimeoutMs ?? 5000;
  if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 100 || requestTimeoutMs > 60_000)
    throw new Error('Meeting integration requestTimeoutMs must be between 100 and 60000');

  logger.debug(`Meeting integration enabled for ${endpoint.origin}`);
  return new HttpMeetingIntegrationTransport(endpoint, token, requestTimeoutMs);
}

export function parseMeetingPlatformConfiguration(value: unknown): MeetingPlatformConfiguration {
  if (!isRecord(value) || Object.keys(value).length !== 2 || value.schemaVersion !== 1 || !Array.isArray(value.channels))
    throw new Error('Meeting Platform configuration response is malformed');
  if (value.channels.length > maximumMeetingPlatformConfigurationChannels)
    throw new Error(`Meeting Platform configuration cannot contain more than ${maximumMeetingPlatformConfigurationChannels} channels`);

  const channelIds = new Set<string>();
  const configuredChannels = new Map<string, MeetingPlatformConfigurationChannel>();
  for (const rawChannel of value.channels) {
    if (
      !isRecord(rawChannel) ||
      Object.keys(rawChannel).length !== 2 ||
      typeof rawChannel.guildId !== 'string' ||
      typeof rawChannel.voiceChannelId !== 'string' ||
      !discordSnowflake.test(rawChannel.guildId) ||
      !discordSnowflake.test(rawChannel.voiceChannelId)
    )
      throw new Error('Meeting Platform configuration contains an invalid channel');

    const key = `${rawChannel.guildId}:${rawChannel.voiceChannelId}`;
    if (configuredChannels.has(key) || channelIds.has(rawChannel.voiceChannelId))
      throw new Error('Meeting Platform configuration contains duplicate channels');
    channelIds.add(rawChannel.voiceChannelId);
    configuredChannels.set(key, { guildId: rawChannel.guildId, voiceChannelId: rawChannel.voiceChannelId });
  }

  const channels = [...configuredChannels.values()].sort(
    (left, right) =>
      (left.guildId < right.guildId ? -1 : left.guildId > right.guildId ? 1 : 0) ||
      (left.voiceChannelId < right.voiceChannelId ? -1 : left.voiceChannelId > right.voiceChannelId ? 1 : 0)
  );
  return { schemaVersion: 1, channels };
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

function assertMatchingLifecycleIdentity(startedEvent: AnyMeetingStartedLifecycleEvent, terminalEvent: AnyMeetingTerminalLifecycleEvent): void {
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

export function dateMillisecondsToIsoOrThrow(value: number, field: string): string {
  const date = new Date(value);
  if (!Number.isSafeInteger(value) || value < 0 || !Number.isFinite(date.valueOf()))
    throw new Error(`Durable cancellation ${field} is outside the JavaScript Date range`);
  return date.toISOString();
}

function createOriginalRecordingJob(input: OriginalRecordingPublicationInput, recordingRoot: string): OriginalRecordingOutboxJob {
  if (input.lifecycleV3Snapshot !== undefined) {
    const lifecycleV3Snapshot = restoreCraigLifecycleV3ProducerFromSnapshot(input.lifecycleV3Snapshot).durableSnapshot();
    if (input.startedEvent.schemaVersion !== 3 || input.terminalEvent.schemaVersion !== 3)
      throw new Error('Lifecycle v3 outbox cannot mix schema versions');
    const startedEvent = lifecycleV3Snapshot.pendingOutbox.find(({ eventId }) => eventId === input.startedEvent.eventId);
    const terminalEvent = lifecycleV3Snapshot.pendingOutbox.find(({ eventId }) => eventId === input.terminalEvent.eventId);
    if (
      startedEvent?.type !== 'meeting.started' ||
      (terminalEvent?.type !== 'meeting.ended' && terminalEvent?.type !== 'meeting.aborted') ||
      JSON.stringify(startedEvent) !== JSON.stringify(input.startedEvent) ||
      JSON.stringify(terminalEvent) !== JSON.stringify(input.terminalEvent)
    )
      throw new Error('Lifecycle v3 outbox events do not match their durable snapshot');
    assertMatchingLifecycleIdentity(startedEvent, terminalEvent);
    const { recordingId, guildId, channelId } = startedEvent;
    assertRecordingId(recordingId);
    assertOriginalSourceFileBase(recordingId, recordingRoot, input.sourceFileBase);
    return withOriginalAdmissionDigest({
      schemaVersion: 3,
      publicationId: `authoritative-recording:v3:${recordingId}`,
      recordingId,
      guildId,
      channelId,
      startedEvent,
      terminalEvent,
      lifecycleV3Snapshot,
      sourceFiles: sourceFileKinds.map((kind) => ({ kind, relativePath: `${recordingId}.ogg.${kind}` }))
    });
  }
  if (input.startedEvent.schemaVersion !== 1 || input.terminalEvent.schemaVersion !== 1)
    throw new Error('Lifecycle v3 publication requires a durable lifecycle snapshot');
  const startedEvent = parseStartedLifecycleEvent(input.startedEvent);
  const terminalEvent = parseTerminalLifecycleEvent(input.terminalEvent);
  assertMatchingLifecycleIdentity(startedEvent, terminalEvent);
  const { recordingId, guildId, channelId } = startedEvent;
  assertRecordingId(recordingId);

  assertOriginalSourceFileBase(recordingId, recordingRoot, input.sourceFileBase);

  return withOriginalAdmissionDigest({
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
  });
}

function withOriginalAdmissionDigest(job: OriginalRecordingOutboxJob): OriginalRecordingOutboxJob {
  return {
    ...job,
    admissionDigestSha256: createHash('sha256').update(canonicalJson(job)).digest('hex')
  };
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object')
    return `{${Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`)
      .join(',')}}`;
  const primitive = JSON.stringify(value);
  if (primitive === undefined) throw new Error('Canonical JSON cannot contain undefined values');
  return primitive;
}

function assertOriginalSourceFileBase(recordingId: string, recordingRoot: string, sourceFileBase: string): void {
  assertRecordingId(recordingId);
  const resolvedRoot = path.resolve(recordingRoot);
  const resolvedBase = path.resolve(sourceFileBase);
  if (path.dirname(resolvedBase) !== resolvedRoot || path.basename(resolvedBase) !== `${recordingId}.ogg`)
    throw new Error('sourceFileBase must identify the recording inside recordingRoot');
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

function syncDirectorySync(directoryPath: string): void {
  const descriptor = openSync(directoryPath, 'r');
  try {
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

async function readOriginalRecordingJob(filePath: string): Promise<OriginalRecordingOutboxJob> {
  const parsed = JSON.parse(await readFile(filePath, 'utf8')) as unknown;
  if (!isRecord(parsed) || (parsed.schemaVersion !== 2 && parsed.schemaVersion !== 3))
    throw new Error('Original recording outbox job has an unsupported schema');
  const allowedKeys = new Set([
    'schemaVersion',
    'publicationId',
    'recordingId',
    'guildId',
    'channelId',
    'startedEvent',
    'terminalEvent',
    'sourceFiles',
    'authoritativeTracks',
    'authoritativeTimelineBasis',
    'lifecycleV3Snapshot',
    'admissionDigestSha256'
  ]);
  if (Object.keys(parsed).some((key) => !allowedKeys.has(key))) throw new Error('Original recording outbox job contains unknown fields');
  const {
    publicationId,
    recordingId,
    guildId,
    channelId,
    startedEvent: rawStartedEvent,
    terminalEvent: rawTerminalEvent,
    sourceFiles,
    authoritativeTracks: rawAuthoritativeTracks,
    authoritativeTimelineBasis: rawAuthoritativeTimelineBasis,
    lifecycleV3Snapshot: rawLifecycleV3Snapshot
  } = parsed;
  const admissionDigestSha256 = parsed.admissionDigestSha256;
  if (
    typeof publicationId !== 'string' ||
    typeof recordingId !== 'string' ||
    typeof guildId !== 'string' ||
    typeof channelId !== 'string' ||
    !Array.isArray(sourceFiles)
  )
    throw new Error('Original recording outbox job is malformed');
  if (admissionDigestSha256 !== undefined && (typeof admissionDigestSha256 !== 'string' || !/^[0-9a-f]{64}$/.test(admissionDigestSha256)))
    throw new Error('Original recording outbox admission digest is invalid');
  assertRecordingId(recordingId);
  const expectedPublicationId = `authoritative-recording:v${parsed.schemaVersion === 3 ? 3 : 1}:${recordingId}`;
  if (publicationId !== expectedPublicationId || !/^\d{16,22}$/.test(guildId) || !/^\d{16,22}$/.test(channelId))
    throw new Error('Original recording outbox job identity is invalid');
  let startedEvent: AnyMeetingStartedLifecycleEvent;
  let terminalEvent: AnyMeetingTerminalLifecycleEvent;
  let lifecycleV3Snapshot: DurableCraigLifecycleV3Snapshot | undefined;
  if (parsed.schemaVersion === 3) {
    lifecycleV3Snapshot = restoreCraigLifecycleV3ProducerFromSnapshot(rawLifecycleV3Snapshot).durableSnapshot();
    const snapshotStarted = lifecycleV3Snapshot.pendingOutbox.find(
      (event) => event.type === 'meeting.started' && isRecord(rawStartedEvent) && event.eventId === rawStartedEvent.eventId
    );
    const snapshotTerminal = lifecycleV3Snapshot.pendingOutbox.find(
      (event) =>
        (event.type === 'meeting.ended' || event.type === 'meeting.aborted') &&
        isRecord(rawTerminalEvent) &&
        event.eventId === rawTerminalEvent.eventId
    );
    if (
      snapshotStarted?.type !== 'meeting.started' ||
      (snapshotTerminal?.type !== 'meeting.ended' && snapshotTerminal?.type !== 'meeting.aborted') ||
      JSON.stringify(snapshotStarted) !== JSON.stringify(rawStartedEvent) ||
      JSON.stringify(snapshotTerminal) !== JSON.stringify(rawTerminalEvent)
    )
      throw new Error('Original recording lifecycle v3 events do not match their durable snapshot');
    startedEvent = snapshotStarted;
    terminalEvent = snapshotTerminal;
  } else {
    if (rawLifecycleV3Snapshot !== undefined) throw new Error('Legacy outbox job cannot claim lifecycle v3 state');
    startedEvent = parseStartedLifecycleEvent(rawStartedEvent);
    terminalEvent = parseTerminalLifecycleEvent(rawTerminalEvent);
  }
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
  if (lifecycleV3Snapshot?.sealedReady !== null && lifecycleV3Snapshot?.sealedReady !== undefined) {
    const ready = lifecycleV3Snapshot.sealedReady;
    if (
      ready.type !== 'recording.authoritative_ready' ||
      authoritativeTracks === undefined ||
      ready.trackCount !== authoritativeTracks.length ||
      ready.endedAt !== terminalEvent.occurredAt ||
      ready.occurredAt !== terminalEvent.occurredAt ||
      ready.sourceFilesChecksumSha256 !== sourceFilesChecksum(normalizedSources)
    )
      throw new Error('Original recording lifecycle v3 seal does not match prepared outbox evidence');
  }

  return {
    schemaVersion: parsed.schemaVersion,
    publicationId,
    recordingId,
    guildId,
    channelId,
    startedEvent,
    terminalEvent,
    ...(lifecycleV3Snapshot === undefined ? {} : { lifecycleV3Snapshot }),
    ...(admissionDigestSha256 === undefined ? {} : { admissionDigestSha256 }),
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

/**
 * Recovery reconstructs participantIds from .users, which now also contains
 * Botik's authoritative playback track. The recording info file carries the
 * actual Craig bot snowflake, so it is the stable exclusion key while the
 * track remains present for authoritative upload preparation.
 */
async function inspectOriginalRecordingBotSpeakerId(filePath: string): Promise<string | undefined> {
  try {
    const parsed = JSON.parse(await readFile(filePath, 'utf8')) as unknown;
    if (!isRecord(parsed) || typeof parsed.clientId !== 'string' || !discordSnowflake.test(parsed.clientId)) return undefined;
    return parsed.clientId;
  } catch {
    // Legacy/incomplete info files still retain all tracks rather than blocking recovery.
    return undefined;
  }
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
  let buffered: Buffer<ArrayBufferLike> = Buffer.alloc(0);
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
