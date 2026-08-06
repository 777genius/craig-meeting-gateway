export interface ConversationPlaybackReconnectTimer {
  schedule(callback: () => void, delayMs: number): unknown;
  cancel(handle: unknown): void;
}

const systemTimer: ConversationPlaybackReconnectTimer = {
  schedule: (callback, delayMs) => setTimeout(callback, delayMs),
  cancel: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>)
};

/** Recording-scoped retry state for the outbound Meeting Platform transport. */
export class ConversationPlaybackReconnect {
  private attempt = 0;
  private pending?: unknown;

  constructor(
    private readonly reconnect: () => void,
    private readonly timer: ConversationPlaybackReconnectTimer = systemTimer,
    private readonly initialDelayMs = 500,
    private readonly maximumDelayMs = 30_000
  ) {}

  connected(): void {
    this.attempt = 0;
    this.cancelPending();
  }

  disconnected(): void {
    if (this.pending !== undefined) return;

    const delayMs = Math.min(this.maximumDelayMs, this.initialDelayMs * 2 ** Math.min(this.attempt, 6));
    this.attempt++;
    this.pending = this.timer.schedule(() => {
      this.pending = undefined;
      this.reconnect();
    }, delayMs);
  }

  stop(): void {
    this.attempt = 0;
    this.cancelPending();
  }

  private cancelPending(): void {
    if (this.pending === undefined) return;
    this.timer.cancel(this.pending);
    this.pending = undefined;
  }
}
