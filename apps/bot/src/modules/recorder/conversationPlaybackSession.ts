import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { WebSocket } from 'ws';

import {
  type CraigPlaybackOpusEncoder,
  CRAIG_PLAYBACK_MAX_PCM_CHUNK_BYTES,
  CraigPlaybackArbiter,
  CraigPlaybackController,
  CraigPlaybackEvent
} from './conversationPlayback';

export const CRAIG_PLAYBACK_MAX_MESSAGE_BYTES = CRAIG_PLAYBACK_MAX_PCM_CHUNK_BYTES * 2;

export interface MeetingPlaybackConfig {
  enabled: boolean;
  endpoint: string;
  tokenFile: string;
  connectionTimeoutMs: number;
}

export interface ConversationPlaybackLogger {
  debug(message: string): void;
  warn(message: string, error?: unknown): void;
}

export interface ConversationPlaybackSocket {
  on(event: 'open', listener: () => void): this;
  on(event: 'message', listener: (data: unknown, isBinary: boolean) => void): this;
  on(event: 'close', listener: (code: number, reason: Buffer) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  send(data: string): void;
  close(code?: number, data?: string | Buffer): void;
}

export interface ConversationPlaybackSocketOptions {
  headers: { Authorization: string };
  handshakeTimeout: number;
  maxPayload: number;
}

export interface ConversationPlaybackSessionOptions {
  config: MeetingPlaybackConfig | undefined;
  recordingId: string;
  guildId: string;
  channelId: string;
  arbiter: CraigPlaybackArbiter;
  logger: ConversationPlaybackLogger;
  socketFactory?: (url: string, options: ConversationPlaybackSocketOptions) => ConversationPlaybackSocket;
  createGatewaySessionId?: () => string;
  createOpusEncoder?: () => CraigPlaybackOpusEncoder;
  now?: () => number;
  onPacketDispatched?: (opusPacket: Buffer) => void;
  onCancellation?: ConstructorParameters<typeof CraigPlaybackController>[0]['onCancellation'];
  onReady?: () => void;
  onClosed?: (reason: ConversationPlaybackCloseReason) => void;
}

export type ConversationPlaybackCloseReason = 'recording-ended' | 'connection-unavailable' | 'transport-disconnected' | 'protocol-violation';

/**
 * The recording-owned outbound transport. It never exposes an inbound Craig
 * endpoint and closes its controller together with the recording or voice
 * connection that owns it.
 */
export class CraigConversationPlaybackSession {
  readonly controller: CraigPlaybackController;
  private ready = false;
  private closed = false;
  private closeNotified = false;

  constructor(
    private readonly socket: ConversationPlaybackSocket,
    controller: CraigPlaybackController,
    private readonly readyEvent: PlaybackSessionReadyEvent,
    private readonly logger: ConversationPlaybackLogger,
    private readonly onReady?: () => void,
    private readonly onClosed?: (reason: ConversationPlaybackCloseReason) => void
  ) {
    this.controller = controller;
    socket.on('open', () => this.onOpen());
    socket.on('message', (data, isBinary) => this.onMessage(data, isBinary));
    socket.on('close', () => this.onRemoteClose());
    socket.on('error', (error) => this.onError(error));
  }

  get isClosed(): boolean {
    return this.closed;
  }

  close(reason: ConversationPlaybackCloseReason): void {
    if (this.closed) return;

    if (reason === 'connection-unavailable') this.controller.connectionUnavailable();
    else this.controller.dispose();

    this.closed = true;
    this.ready = false;
    this.notifyClosed(reason);
    try {
      this.socket.close(1000, reason);
    } catch (error) {
      this.logger.warn('Failed to close Meeting Platform playback transport.', error);
    }
  }

  private onOpen(): void {
    if (this.closed) return;
    this.ready = true;
    this.onReady?.();
    this.sendEvent(this.readyEvent);
  }

  private onMessage(raw: unknown, isBinary: boolean): void {
    if (this.closed) return;
    if (isBinary) return this.closeForProtocolViolation();

    const text = toMessageText(raw);
    if (text === undefined || Buffer.byteLength(text, 'utf8') > CRAIG_PLAYBACK_MAX_MESSAGE_BYTES) return this.closeForProtocolViolation();

    let parsed: unknown;
    try {
      parsed = JSON.parse(text) as unknown;
    } catch {
      return this.closeForProtocolViolation();
    }

    if (!this.controller.handleCommand(parsed)) this.closeForProtocolViolation();
  }

  private onRemoteClose(): void {
    if (this.closed) return;

    this.closed = true;
    this.ready = false;
    this.controller.transportDisconnected();
    this.notifyClosed('transport-disconnected');
  }

  private onError(error: Error): void {
    if (this.closed) return;
    this.logger.warn('Meeting Platform playback transport failed.', error);
    this.closed = true;
    this.ready = false;
    this.controller.transportDisconnected();
    this.notifyClosed('transport-disconnected');
    try {
      this.socket.close(1011, 'transport error');
    } catch {
      // The controller is already closed, and there is no safe retry path here.
    }
  }

  private closeForProtocolViolation(): void {
    if (this.closed) return;

    this.closed = true;
    this.ready = false;
    this.controller.transportDisconnected();
    this.notifyClosed('protocol-violation');
    try {
      this.socket.close(1008, 'invalid playback protocol');
    } catch (error) {
      this.logger.warn('Failed to close invalid Meeting Platform playback transport.', error);
    }
  }

  sendEvent(event: CraigPlaybackEvent | PlaybackSessionReadyEvent): void {
    if (this.closed || !this.ready) return;
    try {
      this.socket.send(JSON.stringify(event));
    } catch (error) {
      this.logger.warn('Failed to send Meeting Platform playback event.', error);
      this.onError(error instanceof Error ? error : new Error('Unknown playback transport send failure'));
    }
  }

  private notifyClosed(reason: ConversationPlaybackCloseReason): void {
    if (this.closeNotified) return;
    this.closeNotified = true;
    this.onClosed?.(reason);
  }
}

interface PlaybackSessionReadyEvent {
  schemaVersion: 1;
  type: 'session-ready';
  recordingId: string;
  guildId: string;
  channelId: string;
  gatewaySessionId: string;
}

export async function createConversationPlaybackSession(
  options: ConversationPlaybackSessionOptions
): Promise<CraigConversationPlaybackSession | undefined> {
  const config = options.config;
  if (!config?.enabled) return undefined;

  assertIdentifier(options.recordingId, 'recordingId');
  assertSnowflake(options.guildId, 'guildId');
  assertSnowflake(options.channelId, 'channelId');

  const endpoint = parseEndpoint(config.endpoint);
  const token = (await readFile(config.tokenFile, 'utf8')).trim();
  if (!token) throw new Error('Meeting playback token file is empty');

  endpoint.searchParams.set('recordingId', options.recordingId);
  const socketFactory = options.socketFactory ?? createWebSocket;
  const socket = socketFactory(endpoint.toString(), {
    headers: { Authorization: `Bearer ${token}` },
    handshakeTimeout: config.connectionTimeoutMs,
    maxPayload: CRAIG_PLAYBACK_MAX_MESSAGE_BYTES
  });

  let sendEvent: (event: CraigPlaybackEvent) => void = () => undefined;
  const controller = new CraigPlaybackController({
    recordingId: options.recordingId,
    arbiter: options.arbiter,
    createOpusEncoder: options.createOpusEncoder,
    now: options.now,
    onPacketDispatched: options.onPacketDispatched,
    onCancellation: options.onCancellation,
    onEvent: (event) => sendEvent(event)
  });
  const gatewaySessionId = options.createGatewaySessionId?.() ?? randomUUID();
  assertIdentifier(gatewaySessionId, 'gatewaySessionId');

  const session = new CraigConversationPlaybackSession(
    socket,
    controller,
    {
      schemaVersion: 1,
      type: 'session-ready',
      recordingId: options.recordingId,
      guildId: options.guildId,
      channelId: options.channelId,
      gatewaySessionId
    },
    options.logger,
    options.onReady,
    options.onClosed
  );
  sendEvent = (event) => session.sendEvent(event);
  return session;
}

function createWebSocket(url: string, options: ConversationPlaybackSocketOptions): ConversationPlaybackSocket {
  return new WebSocket(url, options) as unknown as ConversationPlaybackSocket;
}

function parseEndpoint(value: string): URL {
  let endpoint: URL;
  try {
    endpoint = new URL(value);
  } catch {
    throw new Error('Meeting playback endpoint must be a valid WS(S) URL');
  }
  if (!['ws:', 'wss:'].includes(endpoint.protocol) || endpoint.username || endpoint.password)
    throw new Error('Meeting playback endpoint must be a WS(S) URL without embedded credentials');
  return endpoint;
}

function assertIdentifier(value: string, name: string): void {
  if (value.length < 1 || value.length > 128) throw new Error(`Meeting playback ${name} is invalid`);
}

function assertSnowflake(value: string, name: string): void {
  if (!/^\d{17,20}$/.test(value)) throw new Error(`Meeting playback ${name} is invalid`);
}

function toMessageText(value: unknown): string | undefined {
  if (typeof value === 'string') return value;
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  if (value instanceof ArrayBuffer) return Buffer.from(value).toString('utf8');
  if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('utf8');
  if (Array.isArray(value) && value.every((part) => Buffer.isBuffer(part))) return Buffer.concat(value).toString('utf8');
  return undefined;
}
