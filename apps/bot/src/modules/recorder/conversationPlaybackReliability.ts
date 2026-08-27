import { createHash, randomUUID } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import path from 'node:path';

export const CRAIG_PLAYBACK_DEDUPLICATION_RETENTION_SECONDS = 300;

export interface PlaybackReliabilityIdentity {
  recordingId: string;
  turnId: string;
  attemptId: string;
}

export type PlaybackReliabilityTerminal =
  | { type: 'playback-finished'; finishedAtMs: number }
  | {
      type: 'playback-failed';
      code: 'backpressure' | 'connection-unavailable' | 'invalid-audio' | 'playback-error' | 'transport-disconnected';
      safeMessage: string;
      retryable: boolean;
    };

export type PlaybackReliabilitySnapshot =
  | { status: 'fresh' }
  | { status: 'dispatching'; notAfterUnixMs?: number; reservedAtMs: number }
  | { status: 'started'; notAfterUnixMs?: number; reservedAtMs: number; startedAtMs: number }
  | {
      status: 'terminal';
      notAfterUnixMs?: number;
      reservedAtMs: number;
      startedAtMs?: number;
      terminal: PlaybackReliabilityTerminal;
    };

export interface PlaybackReliabilityStore {
  inspect(identity: Readonly<PlaybackReliabilityIdentity>): PlaybackReliabilitySnapshot;
  reserve(
    identity: Readonly<PlaybackReliabilityIdentity>,
    notAfterUnixMs: number | undefined,
    reservedAtMs: number
  ): { created: boolean; snapshot: PlaybackReliabilitySnapshot };
  authorizeFirstPacket(identity: Readonly<PlaybackReliabilityIdentity>, nowMs: number): boolean;
  markStarted(identity: Readonly<PlaybackReliabilityIdentity>, startedAtMs: number): number;
  markTerminal(
    identity: Readonly<PlaybackReliabilityIdentity>,
    terminal: Readonly<PlaybackReliabilityTerminal>
  ): PlaybackReliabilitySnapshot;
}

type DurableSnapshot = Exclude<PlaybackReliabilitySnapshot, { status: 'fresh' }> & {
  schemaVersion: 1;
  identity: PlaybackReliabilityIdentity;
};

/**
 * Recording-scoped filesystem adapter. The dispatching marker is fsynced before
 * Discord receives the first packet. A crash can therefore lose one greeting,
 * but can never make a retry play the same greeting twice.
 */
export class FilePlaybackReliabilityStore implements PlaybackReliabilityStore {
  constructor(private readonly recordingFileBase: string) {}

  inspect(identity: Readonly<PlaybackReliabilityIdentity>): PlaybackReliabilitySnapshot {
    const filePath = this.filePath(identity);
    if (!existsSync(filePath)) return { status: 'fresh' };
    return this.read(filePath, identity);
  }

  reserve(
    identity: Readonly<PlaybackReliabilityIdentity>,
    notAfterUnixMs: number | undefined,
    reservedAtMs: number
  ): { created: boolean; snapshot: PlaybackReliabilitySnapshot } {
    const existing = this.inspect(identity);
    if (existing.status !== 'fresh') return { created: false, snapshot: existing };

    const snapshot: DurableSnapshot = {
      schemaVersion: 1,
      identity: copyIdentity(identity),
      status: 'dispatching',
      reservedAtMs,
      ...(notAfterUnixMs === undefined ? {} : { notAfterUnixMs })
    };
    const filePath = this.filePath(identity);
    let descriptor: number | undefined;
    try {
      descriptor = openSync(filePath, 'wx', 0o600);
      writeFileSync(descriptor, `${JSON.stringify(snapshot)}\n`, 'utf8');
      fsyncSync(descriptor);
      closeSync(descriptor);
      descriptor = undefined;
      this.fsyncDirectory(filePath);
      return { created: true, snapshot };
    } catch (error) {
      if (descriptor !== undefined) closeSync(descriptor);
      if ((error as NodeJS.ErrnoException).code === 'EEXIST') return { created: false, snapshot: this.read(filePath, identity) };
      throw error;
    }
  }

  authorizeFirstPacket(identity: Readonly<PlaybackReliabilityIdentity>, nowMs: number): boolean {
    const snapshot = this.inspect(identity);
    if (snapshot.status !== 'dispatching') return false;
    return snapshot.notAfterUnixMs === undefined || nowMs < snapshot.notAfterUnixMs;
  }

  markStarted(identity: Readonly<PlaybackReliabilityIdentity>, startedAtMs: number): number {
    const current = this.inspect(identity);
    if (current.status === 'started') return current.startedAtMs;
    if (current.status === 'terminal' && current.startedAtMs !== undefined) return current.startedAtMs;
    if (current.status !== 'dispatching') throw new Error('Playback attempt is not durably reserved for dispatch');

    this.replace(identity, {
      schemaVersion: 1,
      identity: copyIdentity(identity),
      status: 'started',
      reservedAtMs: current.reservedAtMs,
      ...(current.notAfterUnixMs === undefined ? {} : { notAfterUnixMs: current.notAfterUnixMs }),
      startedAtMs
    });
    return startedAtMs;
  }

  markTerminal(
    identity: Readonly<PlaybackReliabilityIdentity>,
    terminal: Readonly<PlaybackReliabilityTerminal>
  ): PlaybackReliabilitySnapshot {
    const current = this.inspect(identity);
    if (current.status === 'terminal') return current;
    const reservedAtMs = current.status === 'fresh' ? terminalTimestamp(terminal) : current.reservedAtMs;
    const snapshot: DurableSnapshot = {
      schemaVersion: 1,
      identity: copyIdentity(identity),
      status: 'terminal',
      reservedAtMs,
      ...(current.status === 'fresh' || current.notAfterUnixMs === undefined ? {} : { notAfterUnixMs: current.notAfterUnixMs }),
      ...(current.status === 'started' ? { startedAtMs: current.startedAtMs } : {}),
      terminal: { ...terminal }
    };
    if (current.status === 'fresh') this.create(identity, snapshot);
    else this.replace(identity, snapshot);
    return snapshot;
  }

  private create(identity: Readonly<PlaybackReliabilityIdentity>, snapshot: DurableSnapshot): void {
    const filePath = this.filePath(identity);
    let descriptor: number | undefined;
    try {
      descriptor = openSync(filePath, 'wx', 0o600);
      writeFileSync(descriptor, `${JSON.stringify(snapshot)}\n`, 'utf8');
      fsyncSync(descriptor);
      closeSync(descriptor);
      descriptor = undefined;
      this.fsyncDirectory(filePath);
    } catch (error) {
      if (descriptor !== undefined) closeSync(descriptor);
      throw error;
    }
  }

  private replace(identity: Readonly<PlaybackReliabilityIdentity>, snapshot: DurableSnapshot): void {
    const filePath = this.filePath(identity);
    const temporaryPath = `${filePath}-tmp-${process.pid}-${randomUUID()}`;
    let descriptor: number | undefined;
    try {
      descriptor = openSync(temporaryPath, 'wx', 0o600);
      writeFileSync(descriptor, `${JSON.stringify(snapshot)}\n`, 'utf8');
      fsyncSync(descriptor);
      closeSync(descriptor);
      descriptor = undefined;
      renameSync(temporaryPath, filePath);
      this.fsyncDirectory(filePath);
    } catch (error) {
      if (descriptor !== undefined) closeSync(descriptor);
      try { unlinkSync(temporaryPath); } catch {}
      throw error;
    }
  }

  private read(filePath: string, identity: Readonly<PlaybackReliabilityIdentity>): DurableSnapshot {
    const value = JSON.parse(readFileSync(filePath, 'utf8')) as unknown;
    if (!isRecord(value) || value.schemaVersion !== 1 || !sameIdentity(value.identity, identity))
      throw new Error('Playback reliability receipt is invalid');
    if (!Number.isSafeInteger(value.reservedAtMs) || (value.reservedAtMs as number) < 0)
      throw new Error('Playback reliability reservation time is invalid');
    if (value.notAfterUnixMs !== undefined && (!Number.isSafeInteger(value.notAfterUnixMs) || (value.notAfterUnixMs as number) < 0))
      throw new Error('Playback reliability deadline is invalid');
    if (value.status === 'dispatching') return value as DurableSnapshot;
    if (value.status === 'started') {
      if (!Number.isSafeInteger(value.startedAtMs) || (value.startedAtMs as number) < 0)
        throw new Error('Playback reliability started receipt is invalid');
      return value as DurableSnapshot;
    }
    if (value.status === 'terminal') {
      if (value.startedAtMs !== undefined && (!Number.isSafeInteger(value.startedAtMs) || (value.startedAtMs as number) < 0))
        throw new Error('Playback reliability terminal started time is invalid');
      assertTerminal(value.terminal);
      return value as DurableSnapshot;
    }
    throw new Error('Playback reliability state is invalid');
  }

  private filePath(identity: Readonly<PlaybackReliabilityIdentity>): string {
    const key = createHash('sha256')
      .update(`${identity.recordingId}\0${identity.turnId}\0${identity.attemptId}`)
      .digest('hex')
      .slice(0, 40);
    // Keep the whole sidecar suffix dot-free so Craig's existing recording
    // cleaner treats it as one exact file type and removes it with the audio.
    return `${this.recordingFileBase}.playback-reliability-${key}`;
  }

  private fsyncDirectory(filePath: string): void {
    const directory = openSync(path.dirname(filePath), 'r');
    try { fsyncSync(directory); } finally { closeSync(directory); }
  }
}

function terminalTimestamp(terminal: Readonly<PlaybackReliabilityTerminal>): number {
  return terminal.type === 'playback-finished' ? terminal.finishedAtMs : 0;
}

function copyIdentity(identity: Readonly<PlaybackReliabilityIdentity>): PlaybackReliabilityIdentity {
  return {
    recordingId: identity.recordingId,
    turnId: identity.turnId,
    attemptId: identity.attemptId
  };
}

function sameIdentity(value: unknown, expected: Readonly<PlaybackReliabilityIdentity>): boolean {
  return isRecord(value) && value.recordingId === expected.recordingId && value.turnId === expected.turnId && value.attemptId === expected.attemptId;
}

function assertTerminal(value: unknown): asserts value is PlaybackReliabilityTerminal {
  if (!isRecord(value) || (value.type !== 'playback-finished' && value.type !== 'playback-failed'))
    throw new Error('Playback reliability terminal receipt is invalid');
  if (value.type === 'playback-finished') {
    if (!Number.isSafeInteger(value.finishedAtMs) || (value.finishedAtMs as number) < 0)
      throw new Error('Playback reliability finish time is invalid');
    return;
  }
  if (
    value.code !== 'backpressure' &&
    value.code !== 'connection-unavailable' &&
    value.code !== 'invalid-audio' &&
    value.code !== 'playback-error' &&
    value.code !== 'transport-disconnected'
  )
    throw new Error('Playback reliability failure code is invalid');
  if (
    typeof value.safeMessage !== 'string' ||
    value.safeMessage.trim().length < 1 ||
    value.safeMessage.length > 512 ||
    typeof value.retryable !== 'boolean'
  )
    throw new Error('Playback reliability failure receipt is invalid');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
