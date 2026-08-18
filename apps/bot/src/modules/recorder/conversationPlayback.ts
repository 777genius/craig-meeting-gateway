import { OpusEncoder } from '@discordjs/opus';

export const CRAIG_PLAYBACK_SAMPLE_RATE_HZ = 48_000;
export const CRAIG_PLAYBACK_MONO_CHANNELS = 1;
export const CRAIG_PLAYBACK_FRAME_SAMPLES = 960;
export const CRAIG_PLAYBACK_MONO_FRAME_BYTES = CRAIG_PLAYBACK_FRAME_SAMPLES * 2;
export const CRAIG_PLAYBACK_STEREO_FRAME_BYTES = CRAIG_PLAYBACK_MONO_FRAME_BYTES * 2;
export const CRAIG_PLAYBACK_MAX_BUFFERED_FRAMES = 100;
export const CRAIG_PLAYBACK_MAX_BUFFERED_PCM_BYTES = CRAIG_PLAYBACK_MAX_BUFFERED_FRAMES * CRAIG_PLAYBACK_MONO_FRAME_BYTES;
export const CRAIG_PLAYBACK_MAX_PCM_CHUNK_BYTES = 19_200;
export const CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS = 253_402_300_799_999;

type CraigPlaybackFailureCode = 'backpressure' | 'connection-unavailable' | 'invalid-audio' | 'playback-error' | 'transport-disconnected';

export interface PlaybackIdentity {
  recordingId: string;
  turnId: string;
  attemptId: string;
}

interface PlaybackStartCommand extends PlaybackIdentity {
  type: 'playback-start';
}

interface PlaybackAudioChunkCommand extends PlaybackIdentity {
  type: 'audio-chunk';
  sequence: number;
  pcm: Buffer;
}

interface PlaybackFinishCommand extends PlaybackIdentity {
  type: 'playback-finish';
}

interface PlaybackCancelCommand extends PlaybackIdentity {
  type: 'playback-cancel';
  schemaVersion: 1 | 2;
  meetingId?: string;
  cancellationObservedAtMs?: number;
  cancellationObservedAt?: string;
  reason: string;
}

type CraigPlaybackCommand = PlaybackStartCommand | PlaybackAudioChunkCommand | PlaybackFinishCommand | PlaybackCancelCommand;

export type CraigPlaybackEvent =
  | {
      schemaVersion: 1;
      type: 'playback-started';
      recordingId: string;
      turnId: string;
      attemptId: string;
      startedAtMs: number;
    }
  | {
      schemaVersion: 1;
      type: 'playback-finished';
      recordingId: string;
      turnId: string;
      attemptId: string;
      finishedAtMs: number;
    }
  | {
      schemaVersion: 1;
      type: 'playback-failed';
      recordingId: string;
      turnId: string;
      attemptId: string;
      code: CraigPlaybackFailureCode;
      safeMessage: string;
      retryable: boolean;
    };

export interface CraigPlaybackVoiceConnection {
  ready: boolean;
  udpSocket: unknown | null | undefined;
  play(input: string, options: { format: string }): void;
  stopPlaying(): void;
  sendAudioFrame(frame: Buffer): void;
  setSpeaking(value: boolean): void;
}

export interface CraigPlaybackOpusEncoder {
  encode(frame: Buffer): Buffer;
}

export interface CraigPlaybackTimer {
  schedule(callback: () => void, delayMs: number): unknown;
  cancel(handle: unknown): void;
}

export interface CraigPlaybackControllerOptions {
  recordingId: string;
  arbiter: CraigPlaybackArbiter;
  onEvent(event: CraigPlaybackEvent): void;
  onPacketDispatched?(opusPacket: Buffer): void;
  /**
   * Synchronously establishes the durable admission fence before the active
   * generation is revoked. Returning false fails closed and leaves playback
   * active so the command can be retried after durability recovers.
   */
  onCancellation(cancellation: Readonly<PlaybackCancelCommand>): boolean;
  /** Consults the durable per-attempt fence before admitting start or PCM. */
  isAttemptRevoked(identity: Readonly<PlaybackIdentity>): boolean;
  /** Records a packet offered after the exact cancelled attempt was revoked. */
  onPostCancellationPacket(identity: Readonly<PlaybackIdentity>): boolean;
  createOpusEncoder?: () => CraigPlaybackOpusEncoder;
  now?: () => number;
  timer?: CraigPlaybackTimer;
}

interface ActivePlayback extends PlaybackIdentity {
  encoder: CraigPlaybackOpusEncoder;
  lastSequence: number | undefined;
  remainder: Buffer;
  started: boolean;
  state: 'receiving' | 'finishing';
  sender?: CraigOpusPacketSender;
}

/**
 * One Discord voice connection can only have one active player. Craig's
 * startup announcement and a conversation turn therefore use this same small
 * arbiter instead of issuing competing VoiceConnection.play calls.
 */
export class CraigPlaybackArbiter {
  private activeConversation: object | undefined;
  private nowRecordingPlaying = false;

  constructor(private readonly getConnection: () => CraigPlaybackVoiceConnection | null) {}

  playNowRecording(filePath: string): boolean {
    if (this.activeConversation !== undefined) return false;

    const connection = this.getConnection();
    if (!connection) return false;

    connection.play(filePath, { format: 'ogg' });
    this.nowRecordingPlaying = true;
    return true;
  }

  startConversation(owner: object): CraigPlaybackVoiceConnection | undefined {
    if (this.activeConversation !== undefined && this.activeConversation !== owner) return undefined;

    const connection = this.getConnection();
    if (!connection || !isDirectAudioReady(connection)) return undefined;

    if (this.nowRecordingPlaying) {
      connection.stopPlaying();
      this.nowRecordingPlaying = false;
    }

    this.activeConversation = owner;
    return connection;
  }

  finishConversation(owner: object): void {
    if (this.activeConversation === owner) this.activeConversation = undefined;
  }

  cancelConversation(owner: object): void {
    if (this.activeConversation !== owner) return;

    this.activeConversation = undefined;
  }
}

/**
 * Dysnomia's Piper treats a Readable `data` event as an enqueue operation, so
 * it cannot prove that the packet reached its UDP sender. This sender owns a
 * small 20 ms queue and reports a packet only after `sendAudioFrame()` accepts
 * that exact packet while the voice UDP socket is ready.
 */
class CraigOpusPacketSender {
  private readonly packets: Buffer[] = [];
  private connection: CraigPlaybackVoiceConnection | undefined;
  private finishRequested = false;
  private closed = false;
  private speaking = false;
  private sending = false;
  private nextDispatchAtMs: number | undefined;
  private timerHandle: unknown;
  private timerScheduled = false;

  constructor(
    private readonly owner: object,
    private readonly arbiter: CraigPlaybackArbiter,
    private readonly now: () => number,
    private readonly timer: CraigPlaybackTimer,
    private readonly onPacketDispatched: (packet: Buffer) => void,
    private readonly onDrained: () => void,
    private readonly onFailure: (code: CraigPlaybackFailureCode, safeMessage: string, retryable: boolean) => void
  ) {}

  get bufferedFrameCount(): number {
    return this.packets.length;
  }

  enqueue(packet: Buffer): void {
    if (this.closed || this.finishRequested) return;
    this.packets.push(Buffer.from(packet));
    this.schedule();
  }

  complete(): void {
    if (this.closed || this.finishRequested) return;
    this.finishRequested = true;
    this.schedule();
  }

  cancel(): void {
    if (this.closed) return;
    this.closed = true;
    this.packets.length = 0;
    this.cancelTimer();
    this.stopSpeaking();
  }

  private schedule(): void {
    if (this.closed || this.sending || this.timerScheduled) return;

    if (this.packets.length === 0) {
      if (this.finishRequested) this.finish();
      return;
    }

    const connection = this.connection ?? this.arbiter.startConversation(this.owner);
    if (!connection) {
      this.fail('connection-unavailable', 'Discord voice playback is unavailable.', true);
      return;
    }
    this.connection = connection;
    if (!isDirectAudioReady(connection)) {
      this.fail('connection-unavailable', 'Discord voice playback is unavailable.', true);
      return;
    }

    const now = currentTimestamp(this.now);
    const dispatchAtMs = this.nextDispatchAtMs ?? now;
    const delayMs = Math.max(0, dispatchAtMs - now);
    if (delayMs > 0) {
      this.timerScheduled = true;
      this.timerHandle = this.timer.schedule(() => {
        this.timerScheduled = false;
        this.timerHandle = undefined;
        this.schedule();
      }, delayMs);
      return;
    }

    this.sendNext(connection);
  }

  private sendNext(connection: CraigPlaybackVoiceConnection): void {
    const packet = this.packets.shift();
    if (!packet) return this.schedule();

    if (!isDirectAudioReady(connection)) {
      this.fail('connection-unavailable', 'Discord voice playback is unavailable.', true);
      return;
    }

    this.sending = true;
    try {
      if (!this.speaking) {
        try {
          connection.setSpeaking(true);
          this.speaking = true;
        } catch {
          // Speaking state is advisory; a direct audio send can still succeed.
        }
      }
      connection.sendAudioFrame(packet);
    } catch {
      this.sending = false;
      this.fail('playback-error', 'Craig could not send Discord playback audio.', true);
      return;
    }

    this.sending = false;
    this.nextDispatchAtMs = currentTimestamp(this.now) + 20;
    try {
      this.onPacketDispatched(packet);
    } catch {
      // The authoritative recorder must never be able to interrupt playback.
    }
    if (!this.closed) this.schedule();
  }

  private finish(): void {
    if (this.closed) return;
    this.closed = true;
    this.stopSpeaking();
    this.onDrained();
  }

  private fail(code: CraigPlaybackFailureCode, safeMessage: string, retryable: boolean): void {
    if (this.closed) return;
    this.closed = true;
    this.packets.length = 0;
    this.cancelTimer();
    this.stopSpeaking();
    this.onFailure(code, safeMessage, retryable);
  }

  private cancelTimer(): void {
    if (!this.timerScheduled) return;
    this.timerScheduled = false;
    this.timer.cancel(this.timerHandle);
    this.timerHandle = undefined;
  }

  private stopSpeaking(): void {
    if (!this.speaking) return;
    this.speaking = false;
    try {
      this.connection?.setSpeaking(false);
    } catch {
      // A failed speaking update does not alter whether a frame was accepted.
    }
  }
}

function isDirectAudioReady(connection: CraigPlaybackVoiceConnection): boolean {
  return connection.ready && connection.udpSocket !== null && connection.udpSocket !== undefined;
}

/**
 * Converts one exact 20 ms mono PCM frame into the stereo S16LE frame expected
 * by Craig's existing Discord Opus encoder.
 */
export function duplicateMonoPcmFrameToStereo(monoFrame: Buffer): Buffer {
  if (monoFrame.byteLength !== CRAIG_PLAYBACK_MONO_FRAME_BYTES) throw new Error(`Expected a ${CRAIG_PLAYBACK_MONO_FRAME_BYTES}-byte mono PCM frame`);

  const stereoFrame = Buffer.allocUnsafe(CRAIG_PLAYBACK_STEREO_FRAME_BYTES);
  for (let sourceOffset = 0, targetOffset = 0; sourceOffset < monoFrame.byteLength; sourceOffset += 2, targetOffset += 4) {
    const sample = monoFrame.readInt16LE(sourceOffset);
    stereoFrame.writeInt16LE(sample, targetOffset);
    stereoFrame.writeInt16LE(sample, targetOffset + 2);
  }
  return stereoFrame;
}

/**
 * Accepts only the versioned Craig playback protocol for one recording. The
 * controller deliberately drops a finish-time sub-frame tail: zero-padding it
 * would invent up to 20 ms of audio timing that Meeting Platform never sent.
 */
export class CraigPlaybackController {
  private readonly createOpusEncoder: () => CraigPlaybackOpusEncoder;
  private readonly now: () => number;
  private readonly timer: CraigPlaybackTimer;
  private active: ActivePlayback | undefined;
  private lastTerminal: PlaybackIdentity | undefined;
  private closed = false;

  constructor(private readonly options: CraigPlaybackControllerOptions) {
    if (typeof options.onCancellation !== 'function' || typeof options.isAttemptRevoked !== 'function' ||
        typeof options.onPostCancellationPacket !== 'function')
      throw new Error('Durable playback cancellation, restart lookup, and post-fence attempt handlers are required');
    this.createOpusEncoder = options.createOpusEncoder ?? (() => new OpusEncoder(CRAIG_PLAYBACK_SAMPLE_RATE_HZ, 2));
    this.now = options.now ?? (() => Date.now());
    this.timer = options.timer ?? {
      schedule: (callback, delayMs) => setTimeout(callback, delayMs),
      cancel: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>)
    };
  }

  /** Returns false for a malformed or cross-recording message that must close the transport. */
  handleCommand(input: unknown): boolean {
    if (this.closed) return true;

    let command: CraigPlaybackCommand;
    try {
      command = parseCraigPlaybackCommand(input);
    } catch {
      const identity = getPlaybackIdentity(input);
      if (identity && this.matchesActive(identity)) this.fail('invalid-audio', 'Playback command did not match schema version 1.', false);
      return false;
    }

    if (command.recordingId !== this.options.recordingId) return false;

    switch (command.type) {
      case 'playback-start':
        return this.handleStart(command);
      case 'audio-chunk':
        return this.handleAudioChunk(command);
      case 'playback-finish':
        return this.handleFinish(command);
      case 'playback-cancel':
        return this.handleCancel(command);
    }
  }

  /** Emits a delivery failure while the outbound transport is still available. */
  connectionUnavailable(): void {
    if (this.closed) return;
    if (this.active) this.fail('connection-unavailable', 'Discord voice playback is unavailable.', true);
    this.closed = true;
  }

  /** Stops local playback without attempting to emit over an already-closed transport. */
  transportDisconnected(): void {
    this.dispose();
  }

  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    const active = this.active;
    this.active = undefined;
    if (!active) return;

    this.rememberTerminal(active);
    active.sender?.cancel();
    this.options.arbiter.cancelConversation(this);
  }

  private handleStart(command: PlaybackStartCommand): boolean {
    if (this.isRevoked(command)) return true;
    if (this.active) {
      if (this.matchesActive(command)) return true;
      this.fail('playback-error', 'A previous playback turn is still active.', true);
      return true;
    }

    if (this.matchesLastTerminal(command)) return true;

    this.active = {
      ...command,
      encoder: this.createOpusEncoder(),
      lastSequence: undefined,
      remainder: Buffer.alloc(0),
      started: false,
      state: 'receiving'
    };
    return true;
  }

  private handleAudioChunk(command: PlaybackAudioChunkCommand): boolean {
    if (this.isRevoked(command)) {
      // This callback is intentionally outside a catch. The packet is not
      // handled unless the exact attempted count is durable.
      if (this.options.onPostCancellationPacket(Object.freeze({
        recordingId: command.recordingId,
        turnId: command.turnId,
        attemptId: command.attemptId
      })) !== true) return false;
      return true;
    }
    const active = this.active;
    if (!active) return this.matchesLastTerminal(command);
    if (!this.matches(active, command)) return true;
    if (active.state !== 'receiving') return true;

    if (
      (active.lastSequence === undefined && command.sequence !== 0) ||
      (active.lastSequence !== undefined && (active.lastSequence === Number.MAX_SAFE_INTEGER || command.sequence !== active.lastSequence + 1))
    ) {
      this.fail('invalid-audio', 'Playback audio chunks arrived out of order.', false);
      return true;
    }

    const bufferedPcmBytes = (active.sender?.bufferedFrameCount ?? 0) * CRAIG_PLAYBACK_MONO_FRAME_BYTES + active.remainder.byteLength;
    if (bufferedPcmBytes + command.pcm.byteLength > CRAIG_PLAYBACK_MAX_BUFFERED_PCM_BYTES) {
      this.fail('backpressure', 'Playback audio exceeded the 2 second buffer limit.', true);
      return true;
    }

    active.lastSequence = command.sequence;
    const joined = active.remainder.byteLength === 0 ? command.pcm : Buffer.concat([active.remainder, command.pcm]);
    let offset = 0;
    while (offset + CRAIG_PLAYBACK_MONO_FRAME_BYTES <= joined.byteLength) {
      const frame = joined.subarray(offset, offset + CRAIG_PLAYBACK_MONO_FRAME_BYTES);
      if (!this.enqueueFrame(active, frame)) return true;
      offset += CRAIG_PLAYBACK_MONO_FRAME_BYTES;
    }
    active.remainder = Buffer.from(joined.subarray(offset));
    return true;
  }

  private handleFinish(command: PlaybackFinishCommand): boolean {
    const active = this.active;
    if (!active) return this.matchesLastTerminal(command);
    if (!this.matches(active, command)) return true;
    if (active.state === 'finishing') return true;

    active.state = 'finishing';
    // Do not pad: an incomplete tail has no complete 20 ms timeline to play.
    active.remainder = Buffer.alloc(0);
    if (active.sender) active.sender.complete();
    else this.complete(active);
    return true;
  }

  private handleCancel(command: PlaybackCancelCommand): boolean {
    const active = this.active;
    // Revocation is an identity operation, not an active-player operation. A
    // cancellation may race start, arrive while another turn owns playback,
    // or be replayed after restart; durability must win all of those races.
    if (this.options.onCancellation(Object.freeze({ ...command })) !== true)
      return false;
    if (active && this.matches(active, command)) this.cancel(active);
    return true;
  }

  private isRevoked(identity: PlaybackIdentity): boolean {
    return this.options.isAttemptRevoked(identity) === true;
  }

  private enqueueFrame(active: ActivePlayback, monoFrame: Buffer): boolean {
    let opusPacket: Buffer;
    try {
      opusPacket = active.encoder.encode(duplicateMonoPcmFrameToStereo(monoFrame));
    } catch {
      this.fail('playback-error', 'Craig could not encode the playback audio.', true);
      return false;
    }

    let sender = active.sender;
    if (!sender) {
      sender = new CraigOpusPacketSender(
        this,
        this.options.arbiter,
        this.now,
        this.timer,
        (packet) => this.markPacketDispatched(active, packet),
        () => this.complete(active),
        (code, safeMessage, retryable) => this.fail(code, safeMessage, retryable)
      );
      active.sender = sender;
    }

    sender.enqueue(opusPacket);
    return this.active === active;
  }

  private markPacketDispatched(active: ActivePlayback, opusPacket: Buffer): void {
    if (this.active !== active) return;
    try {
      this.options.onPacketDispatched?.(opusPacket);
    } catch {
      // The authoritative recorder must never be able to interrupt playback.
    }
    if (this.active !== active || active.started) return;
    active.started = true;
    this.emit({
      schemaVersion: 1,
      type: 'playback-started',
      recordingId: active.recordingId,
      turnId: active.turnId,
      attemptId: active.attemptId,
      startedAtMs: currentTimestamp(this.now)
    });
  }

  private complete(active: ActivePlayback): void {
    if (this.active !== active) return;

    this.active = undefined;
    this.rememberTerminal(active);
    this.options.arbiter.finishConversation(this);
    this.emit({
      schemaVersion: 1,
      type: 'playback-finished',
      recordingId: active.recordingId,
      turnId: active.turnId,
      attemptId: active.attemptId,
      finishedAtMs: currentTimestamp(this.now)
    });
  }

  private cancel(active: ActivePlayback): void {
    if (this.active !== active) return;

    this.active = undefined;
    this.rememberTerminal(active);
    active.sender?.cancel();
    this.options.arbiter.cancelConversation(this);
    this.emit({
      schemaVersion: 1,
      type: 'playback-finished',
      recordingId: active.recordingId,
      turnId: active.turnId,
      attemptId: active.attemptId,
      finishedAtMs: currentTimestamp(this.now)
    });
  }

  private fail(code: CraigPlaybackFailureCode, safeMessage: string, retryable: boolean): void {
    const active = this.active;
    if (!active) return;

    this.active = undefined;
    this.rememberTerminal(active);
    active.sender?.cancel();
    this.options.arbiter.cancelConversation(this);
    this.emit({
      schemaVersion: 1,
      type: 'playback-failed',
      recordingId: active.recordingId,
      turnId: active.turnId,
      attemptId: active.attemptId,
      code,
      safeMessage,
      retryable
    });
  }

  private rememberTerminal(active: PlaybackIdentity): void {
    this.lastTerminal = {
      recordingId: active.recordingId,
      turnId: active.turnId,
      attemptId: active.attemptId
    };
  }

  private matchesActive(identity: PlaybackIdentity): boolean {
    return this.active !== undefined && this.matches(this.active, identity);
  }

  private matchesLastTerminal(identity: PlaybackIdentity): boolean {
    return this.lastTerminal !== undefined && this.matches(this.lastTerminal, identity);
  }

  private matches(left: PlaybackIdentity, right: PlaybackIdentity): boolean {
    return left.recordingId === right.recordingId && left.turnId === right.turnId && left.attemptId === right.attemptId;
  }

  private emit(event: CraigPlaybackEvent): void {
    try {
      this.options.onEvent(event);
    } catch {
      // The session owns transport errors; playback must still release its voice player.
    }
  }
}

function currentTimestamp(now: () => number): number {
  return Math.max(0, Math.trunc(now()));
}

function parseCraigPlaybackCommand(value: unknown): CraigPlaybackCommand {
  if (!isRecord(value)) throw new Error('Playback command must be an object');
  const type = value.type;
  if (typeof type !== 'string') throw new Error('Playback command type is missing');

  switch (type) {
    case 'playback-start': {
      const identity = parseEnvelope(value, ['schemaVersion', 'recordingId', 'turnId', 'attemptId', 'type', 'format', 'sampleRateHz', 'channels']);
      if (value.format !== 'pcm_s16le' || value.sampleRateHz !== CRAIG_PLAYBACK_SAMPLE_RATE_HZ || value.channels !== CRAIG_PLAYBACK_MONO_CHANNELS)
        throw new Error('Playback start format is unsupported');
      return { ...identity, type };
    }
    case 'audio-chunk': {
      const identity = parseEnvelope(value, ['schemaVersion', 'recordingId', 'turnId', 'attemptId', 'type', 'sequence', 'pcmBase64']);
      if (typeof value.sequence !== 'number' || !Number.isSafeInteger(value.sequence) || value.sequence < 0)
        throw new Error('Playback chunk sequence is invalid');
      return {
        ...identity,
        type,
        sequence: value.sequence,
        pcm: decodePcm(value.pcmBase64)
      };
    }
    case 'playback-finish': {
      const identity = parseEnvelope(value, ['schemaVersion', 'recordingId', 'turnId', 'attemptId', 'type']);
      return { ...identity, type };
    }
    case 'playback-cancel': {
      if (value.schemaVersion === 2) {
        if (!hasExactlyKeys(value, ['schemaVersion', 'type', 'meetingId', 'recordingId', 'turnId', 'attemptId', 'cancellationObservedAtMs', 'reason']))
          throw new Error('Playback cancel v2 envelope is invalid');
        const identity = {
          recordingId: parseIdentifier(value.recordingId),
          turnId: parseIdentifier(value.turnId),
          attemptId: parseIdentifier(value.attemptId)
        };
        const meetingId = parseIdentifier(value.meetingId);
        const reason = parseIdentifier(value.reason);
        if (!Number.isSafeInteger(value.cancellationObservedAtMs) || (value.cancellationObservedAtMs as number) < 0 ||
            (value.cancellationObservedAtMs as number) > CRAIG_PLAYBACK_MAX_CANONICAL_TIMESTAMP_MS)
          throw new Error('Playback cancellation observation time is invalid');
        return { ...identity, schemaVersion: 2, type, meetingId, cancellationObservedAtMs: value.cancellationObservedAtMs as number, reason };
      }
      const v1Keys = value.cancellationObservedAt === undefined
        ? ['schemaVersion', 'recordingId', 'turnId', 'attemptId', 'type', 'reason']
        : ['schemaVersion', 'recordingId', 'turnId', 'attemptId', 'type', 'reason', 'cancellationObservedAt'];
      const identity = parseEnvelope(value, v1Keys);
      if (
        value.reason !== 'barge-in' &&
        value.reason !== 'meeting-ended' &&
        value.reason !== 'playback-failed' &&
        value.reason !== 'runtime-shutdown' &&
        value.reason !== 'superseded'
      )
        throw new Error('Playback cancel reason is invalid');
      const reason = value.reason;
      if (value.cancellationObservedAt !== undefined && !isCanonicalInstant(value.cancellationObservedAt))
        throw new Error('Playback cancellation observation time is invalid');
      return {
        ...identity,
        schemaVersion: 1,
        type,
        reason,
        ...(value.cancellationObservedAt === undefined ? {} : { cancellationObservedAt: value.cancellationObservedAt })
      };
    }
    default:
      throw new Error('Playback command type is unsupported');
  }
}

function isCanonicalInstant(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) &&
    new Date(value).toISOString() === value
  );
}

function parseEnvelope(value: Record<string, unknown>, expectedKeys: readonly string[]): PlaybackIdentity {
  if (!hasExactlyKeys(value, expectedKeys) || value.schemaVersion !== 1) throw new Error('Playback command envelope is invalid');

  return {
    recordingId: parseIdentifier(value.recordingId),
    turnId: parseIdentifier(value.turnId),
    attemptId: parseIdentifier(value.attemptId)
  };
}

function decodePcm(value: unknown): Buffer {
  if (typeof value !== 'string' || value.length < 4 || value.length > 25_600 || !isCanonicalBase64(value))
    throw new Error('Playback PCM is not valid base64');

  const pcm = Buffer.from(value, 'base64');
  if (pcm.byteLength === 0 || pcm.byteLength > CRAIG_PLAYBACK_MAX_PCM_CHUNK_BYTES || pcm.byteLength % 2 !== 0)
    throw new Error('Playback PCM has an invalid length');
  return pcm;
}

function isCanonicalBase64(value: string): boolean {
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) return false;
  return Buffer.from(value, 'base64').toString('base64') === value;
}

function getPlaybackIdentity(value: unknown): PlaybackIdentity | undefined {
  if (!isRecord(value)) return undefined;
  try {
    return {
      recordingId: parseIdentifier(value.recordingId),
      turnId: parseIdentifier(value.turnId),
      attemptId: parseIdentifier(value.attemptId)
    };
  } catch {
    return undefined;
  }
}

function parseIdentifier(value: unknown): string {
  if (typeof value !== 'string' || value.length < 1 || value.length > 128) throw new Error('Playback identifier is invalid');
  return value;
}

function hasExactlyKeys(value: Record<string, unknown>, expectedKeys: readonly string[]): boolean {
  const keys = Object.keys(value);
  return keys.length === expectedKeys.length && expectedKeys.every((key) => key in value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
