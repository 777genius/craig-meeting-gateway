import type { Socket } from 'node:dgram';

/** RTP clocks are isolated by receiver generation, participant and actual SSRC. */
export class RecordingMediaClock {
  private readonly speakers = new Map<string, Map<string, { timestamp: number; samples: number }>>();
  private generation = 0;
  private packet?: { source: string; timestamp: number; sequence: number };
  private detach?: () => void;

  /** Fresh voice handshake / receiver only; never called for WS resume. */
  beginEpoch(socket?: Socket | null): void {
    this.detach?.();
    this.detach = undefined;
    this.packet = undefined;
    this.speakers.clear();
    const generation = ++this.generation;
    if (!socket) return;

    // CraigChat/dysnomia cd792c8 emits Opus data synchronously inside the UDP
    // message dispatch, but omits SSRC from its four data-event arguments.
    // Scope the actual RTP header to that dispatch (including nested dispatch),
    // without touching the packet, receiver listeners or decryption pipeline.
    // Select the general emit signature; Socket's last overload is message-only.
    const emit: (this: Socket, event: string | symbol, ...args: unknown[]) => boolean = socket.emit;
    const forwardEmit = (receiver: Socket, event: string | symbol, ...args: unknown[]): boolean => {
      if (event !== 'message') return emit.call(receiver, event, ...args);
      const previous = this.packet;
      const msg = args[0];
      this.packet = Buffer.isBuffer(msg) && msg.length >= 12 && msg[0] >>> 6 === 2 && msg[1] === 0x78
        ? { source: `${generation}:${msg.readUInt32BE(8)}`, timestamp: msg.readUInt32BE(4), sequence: msg.readUInt16BE(2) }
        : undefined;
      try {
        return emit.call(receiver, event, ...args);
      } finally {
        this.packet = previous;
      }
    };
    const scopedEmit: Socket['emit'] = function (this: Socket, event: string | symbol, ...args: unknown[]): boolean {
      return forwardEmit(this, event, ...args);
    };
    socket.emit = scopedEmit;
    this.detach = () => {
      if (socket.emit === scopedEmit) socket.emit = emit;
    };
  }

  packetSource(timestamp: number, sequence: number): string | undefined {
    const packet = this.packet;
    return packet?.timestamp === timestamp && packet.sequence === sequence ? packet.source : undefined;
  }

  map(speakerId: string, rtpTimestamp: number, arrivalSamples: number, source: string): number {
    let sources = this.speakers.get(speakerId);
    if (!sources) this.speakers.set(speakerId, sources = new Map());
    const timestamp = rtpTimestamp >>> 0;
    const previous = sources.get(source);
    // Signed serial arithmetic unwraps RTP at 2^32. Adjacent observations must
    // be less than 2^31 ticks apart. Reordering/duplicates remain visible.
    // Retain old SSRC clocks: a late packet cannot reanchor the replacement.
    const samples = previous === undefined
      ? arrivalSamples
      : previous.samples + ((timestamp - previous.timestamp) | 0);
    sources.set(source, { timestamp, samples });
    return Math.floor(samples / 48);
  }
}
