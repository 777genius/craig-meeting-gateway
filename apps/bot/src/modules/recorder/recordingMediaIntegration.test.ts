import assert from 'node:assert/strict';
import Module, { createRequire } from 'node:module';
import { EventEmitter } from 'node:events';
import { createCipheriv } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { test } from 'node:test';
import { RecordingMediaClock } from './recordingMediaClock';
import type RecordingType from './recording';

// Load the actual Recording methods without initializing Discord, Opus, DB or
// provider integrations. Only the media clock is live in this isolated harness.
function loadRecording(): typeof RecordingType {
  const loader = Module as unknown as { _load: (request: string, parent: { filename?: string }, main: boolean) => unknown };
  const original = loader._load;
  loader._load = (request, parent, main) => {
    if (parent?.filename?.endsWith('/recording.ts')) {
      if (request === './recordingMediaClock') return { RecordingMediaClock };
      if (request === '@discordjs/opus') return { OpusEncoder: class {} };
      if (request === 'dayjs') return { extend() {} };
      if (request === 'nanoid') return { customAlphabet: () => () => 'recording' };
      return {};
    }
    return original(request, parent, main);
  };
  try {
    return (createRequire(__filename)('./recording') as typeof import('./recording')).default;
  } finally {
    loader._load = original;
  }
}

test('Recording.onData tees RTP media time while preserving original bytes, RTP and arrival positions', async () => {
  const Recording = loadRecording();
  const published: Array<{ metadata: any; bytes: Buffer }> = [];
  const queued: Array<{ data: Buffer; timestamp: number; time: number }> = [];
  const recording = Object.create(Recording.prototype) as RecordingType;
  Object.assign(recording, {
    active: true, id: 'recording', startTime: [0, 0], liveMediaClock: new RecordingMediaClock(),
    channel: { id: 'channel', guild: { id: 'guild' } }, userPackets: { B: queued },
    getOrCreateRecordingUser: async () => ({ track: 1 }),
    writeToLog() {}, pushToActivity() {},
    connection: { sendWS() {} },
    recorder: {
      client: { bot: { user: { id: 'bot' } } }, logger: { debug() {} },
      meetingIntegration: { publishPacket(metadata: unknown, bytes: Buffer) { published.push({ metadata, bytes }); return true; } }
    }
  });
  const originalHrtime = process.hrtime;
  let position = 1960732;
  process.hrtime = (() => [0, (position + 0.1) * 20833.333]) as typeof process.hrtime;
  const first = Buffer.from([1, 2, 3]);
  const second = Buffer.from([4, 5, 6]);
  try {
    await recording.onData(first, 'B', 123456, 15408, '1:123');
    position = 1960751;
    await recording.onData(second, 'B', 124416, 15409, '1:123');
    assert.deepEqual(published.map(({ metadata }) => metadata.relativeTimeMs), [40848, 40868]);
    assert.deepEqual(published.map(({ metadata }) => metadata.rtpSequence), [15408, 15409]);
    assert.deepEqual(queued.map(({ time }) => time), [1960732, 1960751]);
    assert.deepEqual(queued.map(({ timestamp }) => timestamp), [123456, 124416]);
    assert.equal(queued[0].data, first);
    assert.equal(queued[1].data, second);
    assert.equal(published[0].bytes, first);
    assert.equal(published[1].bytes, second);
    assert.deepEqual(first, Buffer.from([1, 2, 3]));
    assert.deepEqual(second, Buffer.from([4, 5, 6]));
    position = 2400000;
    await recording.onConnectionResumed();
    await recording.onData(first, 'B', 125376, 15410, '1:123');
    assert.equal(published[2].metadata.relativeTimeMs, 40888);
    await recording.onConnectionReady();
    await recording.onData(first, 'B', 99, 5, '1:123');
    assert.equal(published[3].metadata.relativeTimeMs, 50000);
  } finally {
    process.hrtime = originalHrtime;
  }
});

// Exercise the pinned receiver's actual AES decryption and synchronous data
// emission. Only optional native/provider dependencies are stubbed; no sockets,
// credentials, subprocesses or external connections are created.
function loadOfflineReceiver(): any {
  const evidence = resolve(__dirname, '../../../../../.offline-evidence/dysnomia/lib/voice/VoiceConnection.js');
  const receiverPath = existsSync(evidence) ? evidence : require.resolve('eris/lib/voice/VoiceConnection');
  const loader = Module as any;
  const original = loader._load;
  loader._load = (request: string, parent: any, main: boolean) => {
    if (parent?.filename === receiverPath) {
      if (request === 'sodium-native' || request === 'ws') return {};
      if (request === './Piper') return class {};
      if (request === '../util/Opus') return {};
      if (request === 'eventemitter3') return EventEmitter;
    }
    return original(request, parent, main);
  };
  try {
    const VoiceConnection = createRequire(__filename)(receiverPath);
    return new VoiceConnection('guild', { shared: true, opusOnly: true, daveEncryption: false });
  } finally {
    loader._load = original;
  }
}

test('real pinned UDP receiver carries replacement SSRC through production onData wiring', async () => {
  const Recording = loadRecording();
  const connection = loadOfflineReceiver();
  connection.udpSocket = new EventEmitter();
  connection.mode = 'aead_aes256_gcm_rtpsize';
  connection.sendWS = () => {};
  connection.secret = Buffer.alloc(32, 7); // Synthetic local encryption key.
  // The pinned SPEAKING handler retains the old SSRC mapping on replacement.
  connection.ssrcUserMap = { 11: 'A', 22: 'A' };
  const clock = new RecordingMediaClock();
  const published: any[] = [];
  const queued: any[] = [];
  const recording = Object.create(Recording.prototype);
  Object.assign(recording, {
    active: true, id: 'recording', startTime: [0, 0], liveMediaClock: clock,
    writeToLog() {}, pushToActivity() {},
    channel: { id: 'channel', guild: { id: 'guild' } }, userPackets: { A: queued },
    getOrCreateRecordingUser: async () => ({ track: 1 }),
    recorder: {
      client: { bot: { user: { id: 'bot' } } },
      logger: { debug() {} },
      meetingIntegration: { publishPacket(metadata: any, bytes: Buffer) { published.push({ metadata, bytes }); return true; } }
    }
  });
  // Execute the exact bounded receiver setup from Recording, so dropping the
  // fifth argument or socket binding in production makes this test fail.
  const source = readFileSync(resolve(__dirname, 'recording.ts'), 'utf8');
  const setup = source.match(/if \(!alreadyConnected \|\| !this.connection \|\| !this.receiver\) \{([\s\S]*?)\n {4}\}/)![1];
  new Function('connection', setup).call(recording, connection);
  const receiver = recording.receiver;
  let socket = connection.udpSocket;
  const originalHrtime = process.hrtime;
  let position = 48000;
  process.hrtime = (() => [0, (position + 0.1) * 20833.333]) as typeof process.hrtime;
  const opus = Buffer.from([1, 2, 3]);
  function send(ssrc: number, timestamp: number, sequence: number) {
    const header = Buffer.alloc(12);
    header[0] = 0x80; header[1] = 0x78;
    header.writeUInt16BE(sequence, 2); header.writeUInt32BE(timestamp, 4); header.writeUInt32BE(ssrc, 8);
    const nonce = Buffer.alloc(12);
    nonce.writeUInt32BE(sequence);
    const cipher = createCipheriv('aes-256-gcm', connection.secret, nonce);
    cipher.setAAD(header);
    const packet = Buffer.concat([header, cipher.update(opus), cipher.final(), cipher.getAuthTag(), nonce.subarray(0, 4)]);
    const before = Buffer.from(packet);
    socket.emit('message', packet);
    assert.deepEqual(packet, before);
  }
  try {
    send(11, 1000000, 1);
    position = 480000;
    send(22, 100, 2);
    position++;
    send(11, 1000960, 3);
    position++;
    send(22, 1060, 4);
    await Promise.resolve();
    assert.equal(recording.receiver, receiver);
    assert.equal(connection.udpSocket, socket);
    assert.deepEqual(published.map(p => p.metadata.relativeTimeMs), [1000, 10000, 1020, 10020]);
    assert.deepEqual(published.map(p => p.metadata.rtpSequence), [1, 2, 3, 4]);
    for (const packet of queued) {
      assert.deepEqual(packet.data, opus);
      assert.ok(published.some(p => p.bytes === packet.data));
    }
    assert.deepEqual(queued.map(p => p.time).sort((a, b) => a - b), [48000, 480000, 480001, 480002]);
    assert.equal(queued.length, 4);
    position = 960000;
    await recording.onConnectionResumed();
    send(22, 2020, 5);
    assert.equal(published[4].metadata.relativeTimeMs, 10040);
    connection.udpSocket = new EventEmitter();
    await recording.onConnectionReady();
    connection.registerReceiveEventHandler();
    // New socket with the same SSRC and a fresh RTP origin.
    socket = connection.udpSocket;
    send(22, 100, 6);
    assert.equal(published[5].metadata.relativeTimeMs, 20000);
  } finally {
    process.hrtime = originalHrtime;
    clock.beginEpoch();
  }
});
