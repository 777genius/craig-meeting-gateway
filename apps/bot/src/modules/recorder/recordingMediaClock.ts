/** A per-speaker RTP clock, anchored once to Craig's recording-origin samples. */
export class RecordingMediaClock {
  private readonly speakers = new Map<string, { timestamp: number; samples: number }>();

  /** Called for a new receiver / fresh voice handshake, never for WS resume. */
  beginEpoch(): void {
    this.speakers.clear();
  }

  map(speakerId: string, rtpTimestamp: number, arrivalSamples: number): number {
    const timestamp = rtpTimestamp >>> 0;
    const previous = this.speakers.get(speakerId);
    // Signed serial arithmetic unwraps RTP at 2^32. As with RTP serial
    // comparison, adjacent observations must be less than 2^31 ticks apart.
    // Backward/duplicate media remains backward/duplicate: never clamp it or
    // mistake it for a new epoch. Only an explicit connection boundary resets.
    const samples = previous === undefined
      ? arrivalSamples
      : previous.samples + ((timestamp - previous.timestamp) | 0);
    this.speakers.set(speakerId, { timestamp, samples });
    return Math.floor(samples / 48);
  }
}
