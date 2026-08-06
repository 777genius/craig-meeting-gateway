import assert from 'node:assert/strict';
import { test } from 'node:test';

import { ConversationPlaybackReconnect, ConversationPlaybackReconnectTimer } from './conversationPlaybackReconnect';

class FakeTimer implements ConversationPlaybackReconnectTimer {
  readonly delays: number[] = [];
  private pending?: { callback: () => void; cancelled: boolean };

  schedule(callback: () => void, delayMs: number): unknown {
    this.delays.push(delayMs);
    this.pending = { callback, cancelled: false };
    return this.pending;
  }

  cancel(handle: unknown): void {
    (handle as { cancelled: boolean }).cancelled = true;
  }

  fire(): void {
    const pending = this.pending;
    this.pending = undefined;
    if (pending && !pending.cancelled) pending.callback();
  }
}

test('reconnects with bounded backoff and only one pending retry', () => {
  const timer = new FakeTimer();
  let reconnects = 0;
  const retry = new ConversationPlaybackReconnect(() => reconnects++, timer, 500, 2_000);

  retry.disconnected();
  retry.disconnected();
  assert.deepEqual(timer.delays, [500]);
  timer.fire();
  assert.equal(reconnects, 1);

  retry.disconnected();
  timer.fire();
  retry.disconnected();
  timer.fire();
  retry.disconnected();
  assert.deepEqual(timer.delays, [500, 1_000, 2_000, 2_000]);
});

test('successful connection resets backoff and recording stop cancels retry', () => {
  const timer = new FakeTimer();
  let reconnects = 0;
  const retry = new ConversationPlaybackReconnect(() => reconnects++, timer, 500, 2_000);

  retry.disconnected();
  timer.fire();
  retry.connected();
  retry.disconnected();
  assert.deepEqual(timer.delays, [500, 500]);

  retry.stop();
  timer.fire();
  assert.equal(reconnects, 1);
});
