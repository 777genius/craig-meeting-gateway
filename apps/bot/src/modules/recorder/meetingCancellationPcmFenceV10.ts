import { createHash } from 'node:crypto';

import type { RecordingUser } from './recording';

export const meetingCancellationPcmFenceCapabilityId = 'meeting.cancellation.pcm-fence.v10' as const;
export const meetingCancellationPcmFenceEventType = 'meeting.playback_cancelled' as const;

export type CanonicalMeetingCancelReason = 'barge-in' | 'disconnect' | 'meeting-ended';

export interface BotikTrackObject {
  speakerId: string;
  trackNumber: number;
  packetCursor: number;
}

export interface DurableBotikTrackProof {
  track: BotikTrackObject;
  checksumSha256: string;
  sizeBytes: number;
}

export interface MeetingCancellationPcmFenceReceiptV10 {
  schemaVersion: 10;
  type: typeof meetingCancellationPcmFenceEventType;
  capabilityId: typeof meetingCancellationPcmFenceCapabilityId;
  meetingId: string;
  turnId: string;
  cancelReason: CanonicalMeetingCancelReason;
  cancelledAt: string;
  lastAcceptedFactualPcmSequence: number | null;
  lastAcceptedFactualPcmAt: string | null;
  noFactualPcmAcceptedAfterFence: true;
  botikTrack: BotikTrackObject;
  botikTrackChecksumSha256: string;
  botikTrackSizeBytes: number;
  playbackGeneration: number;
}

export interface DurableMeetingCancellationPcmFenceSnapshotV10 {
  schemaVersion: 10;
  meetingId: string;
  turnId: string;
  playbackGeneration: number;
  lastAcceptedFactualPcmSequence: number | null;
  lastAcceptedFactualPcmAt: string | null;
  fence: { cancelReason: CanonicalMeetingCancelReason; cancelledAt: string } | null;
  receipt: MeetingCancellationPcmFenceReceiptV10 | null;
}

export type FactualPcmAdmission = 'accepted' | 'duplicate' | 'late' | 'conflict';
export type CancellationAdmission = 'accepted' | 'duplicate' | 'conflict';

export interface CancellationPcmFenceDurability {
  /** Must resolve only after the exact track bytes and their containing directory are durable. */
  flushAndChecksum(track: BotikTrackObject): Promise<DurableBotikTrackProof>;
  /** Atomically persists the snapshot before a receipt can be emitted. */
  persist(snapshot: DurableMeetingCancellationPcmFenceSnapshotV10): Promise<void>;
}

export interface CancellationPcmFenceCollector {
  emit(event: MeetingCancellationPcmFenceReceiptV10): Promise<void> | void;
}

/**
 * Fail-closed producer for the collector's schema-v10 cancellation receipt.
 * Receipt creation is deliberately separate from PCM admission: no receipt is
 * observable until both the track flush/checksum and snapshot persistence have
 * succeeded. Unknown flush outcomes are failures and are safe to retry.
 */
export class MeetingCancellationPcmFenceV10 {
  private lastSequence: number | null = null;
  private lastAcceptedAt: string | null = null;
  private fence: { cancelReason: CanonicalMeetingCancelReason; cancelledAt: string } | null = null;
  private receipt: MeetingCancellationPcmFenceReceiptV10 | null = null;
  private receiptEmitted = false;
  private finalizing: Promise<MeetingCancellationPcmFenceReceiptV10> | null = null;

  constructor(
    readonly meetingId: string,
    readonly turnId: string,
    readonly playbackGeneration: number,
    private readonly botikTrack: BotikTrackObject,
    private readonly durability: CancellationPcmFenceDurability,
    private readonly collector: CancellationPcmFenceCollector
  ) {
    assertIdentifier(meetingId, 'meetingId');
    assertIdentifier(turnId, 'turnId');
    assertGeneration(playbackGeneration);
    assertTrack(botikTrack);
    this.botikTrack = freezeTrack(botikTrack);
  }

  acceptFactualPcm(sequence: number, acceptedAt: string): FactualPcmAdmission {
    assertSequence(sequence);
    const canonicalAt = canonicalTimestamp(acceptedAt, 'acceptedAt');
    if (this.fence !== null) return 'late';
    if (this.lastSequence !== null && sequence === this.lastSequence)
      return canonicalAt === this.lastAcceptedAt ? 'duplicate' : 'conflict';
    if (sequence !== (this.lastSequence ?? -1) + 1) return 'conflict';
    if (this.lastAcceptedAt !== null && canonicalAt < this.lastAcceptedAt) return 'conflict';
    this.lastSequence = sequence;
    this.lastAcceptedAt = canonicalAt;
    return 'accepted';
  }

  cancel(reason: CanonicalMeetingCancelReason, cancelledAt: string): CancellationAdmission {
    assertReason(reason);
    const canonicalAt = canonicalTimestamp(cancelledAt, 'cancelledAt');
    if (this.lastAcceptedAt !== null && canonicalAt < this.lastAcceptedAt) return 'conflict';
    if (this.fence !== null)
      return this.fence.cancelReason === reason && this.fence.cancelledAt === canonicalAt ? 'duplicate' : 'conflict';
    this.fence = Object.freeze({ cancelReason: reason, cancelledAt: canonicalAt });
    return 'accepted';
  }

  async finalize(): Promise<MeetingCancellationPcmFenceReceiptV10> {
    if (this.receipt !== null) {
      if (!this.receiptEmitted) {
        await this.collector.emit(this.receipt);
        this.receiptEmitted = true;
      }
      return this.receipt;
    }
    if (this.fence === null) throw new Error('Cancellation PCM fence has not been established');
    if (this.finalizing !== null) return this.finalizing;
    const pending = this.finalizeOnce();
    this.finalizing = pending;
    try {
      return await pending;
    } finally {
      if (this.receipt === null) this.finalizing = null;
    }
  }

  snapshot(): DurableMeetingCancellationPcmFenceSnapshotV10 {
    return deepFreeze({
      schemaVersion: 10,
      meetingId: this.meetingId,
      turnId: this.turnId,
      playbackGeneration: this.playbackGeneration,
      lastAcceptedFactualPcmSequence: this.lastSequence,
      lastAcceptedFactualPcmAt: this.lastAcceptedAt,
      fence: this.fence === null ? null : { ...this.fence },
      receipt: this.receipt === null ? null : { ...this.receipt, botikTrack: { ...this.receipt.botikTrack } }
    });
  }

  private async finalizeOnce(): Promise<MeetingCancellationPcmFenceReceiptV10> {
    const fence = this.fence!;
    const proof = await this.durability.flushAndChecksum(this.botikTrack);
    assertProof(proof, this.botikTrack);
    const receipt = deepFreeze({
      schemaVersion: 10 as const,
      type: meetingCancellationPcmFenceEventType,
      capabilityId: meetingCancellationPcmFenceCapabilityId,
      meetingId: this.meetingId,
      turnId: this.turnId,
      cancelReason: fence.cancelReason,
      cancelledAt: fence.cancelledAt,
      lastAcceptedFactualPcmSequence: this.lastSequence,
      lastAcceptedFactualPcmAt: this.lastAcceptedAt,
      noFactualPcmAcceptedAfterFence: true as const,
      botikTrack: { ...proof.track },
      botikTrackChecksumSha256: proof.checksumSha256,
      botikTrackSizeBytes: proof.sizeBytes,
      playbackGeneration: this.playbackGeneration
    });
    const snapshot = this.snapshotWithReceipt(receipt);
    await this.durability.persist(snapshot);
    this.receipt = receipt;
    await this.collector.emit(receipt);
    this.receiptEmitted = true;
    return receipt;
  }

  private snapshotWithReceipt(receipt: MeetingCancellationPcmFenceReceiptV10): DurableMeetingCancellationPcmFenceSnapshotV10 {
    return deepFreeze({ ...this.snapshot(), receipt });
  }

  static restore(
    input: unknown,
    botikTrack: BotikTrackObject,
    durability: CancellationPcmFenceDurability,
    collector: CancellationPcmFenceCollector
  ): MeetingCancellationPcmFenceV10 {
    const snapshot = parseSnapshot(input, botikTrack);
    const producer = new MeetingCancellationPcmFenceV10(
      snapshot.meetingId,
      snapshot.turnId,
      snapshot.playbackGeneration,
      botikTrack,
      durability,
      collector
    );
    producer.lastSequence = snapshot.lastAcceptedFactualPcmSequence;
    producer.lastAcceptedAt = snapshot.lastAcceptedFactualPcmAt;
    producer.fence = snapshot.fence;
    producer.receipt = snapshot.receipt;
    return producer;
  }
}

export function botikTrackObject(user: RecordingUser): BotikTrackObject {
  return freezeTrack({ speakerId: user.id, trackNumber: user.track, packetCursor: user.packet });
}

export function cancellationReceiptDigest(receipt: MeetingCancellationPcmFenceReceiptV10): string {
  return createHash('sha256').update(canonicalJson(receipt)).digest('hex');
}

function parseSnapshot(input: unknown, expectedTrack: BotikTrackObject): DurableMeetingCancellationPcmFenceSnapshotV10 {
  if (!isRecord(input) || input.schemaVersion !== 10) throw new Error('Cancellation PCM fence snapshot is malformed');
  const keys = Object.keys(input).sort().join(',');
  if (keys !== 'fence,lastAcceptedFactualPcmAt,lastAcceptedFactualPcmSequence,meetingId,playbackGeneration,receipt,schemaVersion,turnId')
    throw new Error('Cancellation PCM fence snapshot contains unknown fields');
  if (typeof input.meetingId !== 'string' || typeof input.turnId !== 'string' || typeof input.playbackGeneration !== 'number')
    throw new Error('Cancellation PCM fence snapshot identity is malformed');
  const producer = new MeetingCancellationPcmFenceV10(
    input.meetingId,
    input.turnId,
    input.playbackGeneration,
    expectedTrack,
    { flushAndChecksum: async () => Promise.reject(), persist: async () => undefined },
    { emit: () => undefined }
  );
  const sequence = input.lastAcceptedFactualPcmSequence;
  if (sequence !== null) assertSequence(sequence);
  const acceptedAt = input.lastAcceptedFactualPcmAt;
  if ((sequence === null) !== (acceptedAt === null)) throw new Error('Cancellation PCM fence factual PCM evidence is incomplete');
  if (acceptedAt !== null) canonicalTimestamp(acceptedAt, 'lastAcceptedFactualPcmAt');
  const fence = input.fence;
  if (fence !== null) {
    if (!isRecord(fence) || Object.keys(fence).sort().join(',') !== 'cancelReason,cancelledAt') throw new Error('Cancellation PCM fence is malformed');
    assertReason(fence.cancelReason);
    canonicalTimestamp(fence.cancelledAt, 'cancelledAt');
    if (acceptedAt !== null && fence.cancelledAt < acceptedAt) throw new Error('Cancellation PCM fence precedes accepted PCM');
  }
  const receipt = input.receipt;
  if (receipt !== null) {
    assertReceipt(receipt, producer.meetingId, producer.turnId, producer.playbackGeneration, expectedTrack);
    if (fence === null || receipt.cancelReason !== fence.cancelReason || receipt.cancelledAt !== fence.cancelledAt)
      throw new Error('Cancellation receipt does not bind its fence');
    if (receipt.lastAcceptedFactualPcmSequence !== sequence || receipt.lastAcceptedFactualPcmAt !== acceptedAt)
      throw new Error('Cancellation receipt does not bind its factual PCM evidence');
  }
  return deepFreeze({
    schemaVersion: 10,
    meetingId: producer.meetingId,
    turnId: producer.turnId,
    playbackGeneration: producer.playbackGeneration,
    lastAcceptedFactualPcmSequence: sequence as number | null,
    lastAcceptedFactualPcmAt: acceptedAt as string | null,
    fence: fence as DurableMeetingCancellationPcmFenceSnapshotV10['fence'],
    receipt: receipt as MeetingCancellationPcmFenceReceiptV10 | null
  });
}

function assertReceipt(value: unknown, meetingId: string, turnId: string, generation: number, track: BotikTrackObject): asserts value is MeetingCancellationPcmFenceReceiptV10 {
  if (!isRecord(value) || value.schemaVersion !== 10 || value.type !== meetingCancellationPcmFenceEventType || value.capabilityId !== meetingCancellationPcmFenceCapabilityId)
    throw new Error('Cancellation receipt is malformed');
  if (
    Object.keys(value).sort().join(',') !==
    'botikTrack,botikTrackChecksumSha256,botikTrackSizeBytes,cancelReason,cancelledAt,capabilityId,lastAcceptedFactualPcmAt,lastAcceptedFactualPcmSequence,meetingId,noFactualPcmAcceptedAfterFence,playbackGeneration,schemaVersion,turnId,type'
  )
    throw new Error('Cancellation receipt contains unknown fields');
  if (value.meetingId !== meetingId || value.turnId !== turnId || value.playbackGeneration !== generation) throw new Error('Cancellation receipt identity is invalid');
  assertReason(value.cancelReason);
  canonicalTimestamp(value.cancelledAt, 'cancelledAt');
  if (value.noFactualPcmAcceptedAfterFence !== true) throw new Error('Cancellation receipt lacks its PCM fence proof');
  if (value.lastAcceptedFactualPcmSequence !== null) assertSequence(value.lastAcceptedFactualPcmSequence);
  if ((value.lastAcceptedFactualPcmSequence === null) !== (value.lastAcceptedFactualPcmAt === null))
    throw new Error('Cancellation receipt factual PCM evidence is incomplete');
  if (value.lastAcceptedFactualPcmAt !== null) canonicalTimestamp(value.lastAcceptedFactualPcmAt, 'lastAcceptedFactualPcmAt');
  assertProof({ track: value.botikTrack, checksumSha256: value.botikTrackChecksumSha256, sizeBytes: value.botikTrackSizeBytes }, track);
}

function assertProof(proof: DurableBotikTrackProof, expected: BotikTrackObject): void {
  if (!isRecord(proof)) throw new Error('Durable Botik track proof is missing');
  assertTrack(proof.track);
  if (canonicalJson(proof.track) !== canonicalJson(expected)) throw new Error('Durable Botik track proof belongs to another track');
  if (typeof proof.checksumSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(proof.checksumSha256)) throw new Error('Durable Botik track checksum is invalid');
  if (!Number.isSafeInteger(proof.sizeBytes) || proof.sizeBytes < 0) throw new Error('Durable Botik track size is invalid');
}

function assertTrack(value: unknown): asserts value is BotikTrackObject {
  if (!isRecord(value) || typeof value.speakerId !== 'string') throw new Error('Botik track object is invalid');
  assertIdentifier(value.speakerId, 'speakerId');
  if (!Number.isSafeInteger(value.trackNumber) || value.trackNumber < 1 || !Number.isSafeInteger(value.packetCursor) || value.packetCursor < 2)
    throw new Error('Botik track object is invalid');
}

function freezeTrack(track: BotikTrackObject): BotikTrackObject {
  assertTrack(track);
  return Object.freeze({ ...track });
}

function assertReason(value: unknown): asserts value is CanonicalMeetingCancelReason {
  if (value !== 'barge-in' && value !== 'disconnect' && value !== 'meeting-ended') throw new Error('Cancellation reason is not canonical');
}

function assertSequence(value: unknown): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw new Error('Factual PCM sequence is invalid');
}

function assertGeneration(value: number): void {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error('Playback generation must be a positive integer');
}

function assertIdentifier(value: string, name: string): void {
  if (typeof value !== 'string' || value.length < 1 || value.length > 128 || !/^[A-Za-z0-9._:-]+$/.test(value)) throw new Error(`${name} is invalid`);
}

function canonicalTimestamp(value: unknown, name: string): string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) || new Date(value).toISOString() !== value)
    throw new Error(`${name} is not a canonical timestamp`);
  return value;
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (isRecord(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function deepFreeze<T>(value: T): T {
  if (typeof value === 'object' && value !== null) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}
