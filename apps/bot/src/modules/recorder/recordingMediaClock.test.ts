import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventEmitter } from 'node:events';
import type { Socket } from 'node:dgram';
import { RecordingMediaClock } from './recordingMediaClock';

test('RTP sequences 15408/15409 advance 20ms despite exact bunched Craig arrivals', () => {
  const clock = new RecordingMediaClock();
  const observations = [
    { sequence: 15408, timestamp: 123456, arrival: 1960732 },
    { sequence: 15409, timestamp: 124416, arrival: 1960751 }
  ];
  assert.deepEqual(observations.map(({ arrival }) => Math.trunc(arrival / 48)), [40848, 40848]);
  assert.deepEqual(observations.map(({ timestamp, arrival }) => clock.map('B', timestamp, arrival, '1:123')), [40848, 40868]);
});

test('wrap and real missing-media gaps preserve RTP progression, independent of arrival jitter', () => {
  const clock = new RecordingMediaClock();
  assert.equal(clock.map('A', 0xfffffe20, 48000, '1:123'), 1000);
  assert.equal(clock.map('A', 480, 48001, '1:123'), 1020);
  assert.equal(clock.map('A', 480 + 960 * 22, 48002, '1:123'), 1460);
  assert.equal(clock.map('B', 100, 96000, '1:123'), 2000);
  assert.equal(clock.map('A', 480 + 960 * 23, 150000, '1:123'), 1480);
});

test('explicit reconnect epoch reanchors each speaker to the continuing recording origin', () => {
  const clock = new RecordingMediaClock();
  clock.map('A', 123456, 48000, '1:123');
  clock.map('B', 654321, 96000, '1:123');
  // A resumed connection keeps its RTP epoch, even with an arrival delay.
  assert.equal(clock.map('A', 124416, 240000, '1:123'), 1020);
  clock.beginEpoch();
  assert.equal(clock.map('A', 50, 480000, '1:123'), 10000);
  assert.equal(clock.map('A', 1010, 480001, '1:123'), 10020);
  assert.equal(clock.map('B', 99, 528000, '1:123'), 11000);
});

test('duplicates and reordered timestamps remain visible, without artificial monotonic clamping', () => {
  const clock = new RecordingMediaClock();
  assert.equal(clock.map('A', 1000, 48000, '1:123'), 1000);
  assert.equal(clock.map('A', 1960, 48001, '1:123'), 1020);
  assert.equal(clock.map('A', 1960, 49000, '1:123'), 1020);
  assert.equal(clock.map('A', 1000, 50000, '1:123'), 1000);
  assert.equal(clock.map('A', 2920, 51000, '1:123'), 1040);
});

test('participant replacement SSRC anchors without bot reconnect; late old SSRC stays isolated', () => {
  const clock = new RecordingMediaClock();
  assert.equal(clock.map('A', 1000000, 48000, '1:11'), 1000);
  assert.equal(clock.map('A', 100, 480000, '1:22'), 10000);
  assert.equal(clock.map('A', 1000960, 480001, '1:11'), 1020);
  assert.equal(clock.map('A', 1060, 480002, '1:22'), 10020);
  assert.equal(clock.map('A', 1060, 600000, '1:22'), 10020);
  assert.equal(clock.map('A', 100, 600001, '1:22'), 10000);
  assert.equal(clock.map('A', 1060 + 960 * 22, 600002, '1:22'), 10460);
});

test('packet scope is synchronous and restored on nesting, errors and receiver generation changes', () => {
  const clock = new RecordingMediaClock();
  const socket = new EventEmitter() as Socket;
  const originalEmit = socket.emit;
  const packet = Buffer.alloc(12);
  packet[0] = 0x80; packet[1] = 0x78;
  packet.writeUInt32BE(11, 8);
  clock.beginEpoch(socket);
  let firstSource: string | undefined;
  socket.on('message', (msg: Buffer) => {
    if (msg.length < 12) {
      assert.equal(clock.packetSource(0, 0), undefined);
      return;
    }
    const source = clock.packetSource(0, 0)!;
    firstSource ??= source;
    socket.emit('message', Buffer.alloc(1));
    assert.equal(clock.packetSource(0, 0), source);
    assert.equal(clock.packetSource(1, 0), undefined);
    assert.equal(clock.packetSource(0, 1), undefined);
  });
  socket.emit('message', packet);
  assert.equal(clock.packetSource(0, 0), undefined);
  socket.once('message', () => { throw new Error('receiver failure'); });
  assert.throws(() => socket.emit('message', packet), /receiver failure/);
  assert.equal(clock.packetSource(0, 0), undefined);
  const replacement = new EventEmitter() as Socket;
  clock.beginEpoch(replacement);
  assert.equal(socket.emit, originalEmit);
  replacement.on('message', () => assert.notEqual(clock.packetSource(0, 0), firstSource));
  replacement.emit('message', packet);
  clock.beginEpoch();
  assert.equal(replacement.emit, originalEmit);
});

test('scoped emit forwards every argument, dynamic this and the original boolean result', () => {
  const clock = new RecordingMediaClock();
  const socket = new EventEmitter() as Socket;
  const receiver = new EventEmitter() as Socket;
  const calls: { receiver: Socket; args: unknown[] }[] = [];
  let result = false;
  socket.emit = function (this: Socket, event: string | symbol, ...args: unknown[]): boolean {
    calls.push({ receiver: this, args: [event, ...args] });
    assert.equal(clock.packetSource(1234, 56), event === 'message' ? '1:11' : undefined);
    return result;
  };
  const originalEmit = socket.emit;
  clock.beginEpoch(socket);
  const emit: (this: Socket, event: string | symbol, ...args: unknown[]) => boolean = socket.emit;
  const packet = Buffer.alloc(12);
  packet[0] = 0x80; packet[1] = 0x78;
  packet.writeUInt32BE(11, 8);
  packet.writeUInt32BE(1234, 4);
  packet.writeUInt16BE(56, 2);
  const rinfo = { address: '127.0.0.1', family: 'IPv4', port: 12345, size: packet.length };
  const extra = {};
  for (const returned of [false, true]) {
    result = returned;
    const events: [string | symbol, ...unknown[]][] = [
      ['close'],
      ['custom', extra, undefined, packet],
      [Symbol('custom'), packet, rinfo, extra],
      ['message', packet, rinfo, extra, undefined]
    ];
    for (const args of events) {
      assert.equal(emit.call(receiver, ...args), returned);
      const call = calls[calls.length - 1];
      assert.equal(call.receiver, receiver);
      assert.equal(call.args.length, args.length);
      args.forEach((arg, index) => assert.equal(call.args[index], arg));
      assert.equal(clock.packetSource(1234, 56), undefined);
    }
  }
  clock.beginEpoch();
  assert.equal(socket.emit, originalEmit);
});
