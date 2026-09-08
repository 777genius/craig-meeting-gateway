import assert from 'node:assert/strict';
import Module from 'node:module';
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
    return require('./recording').default;
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
    await recording.onData(first, 'B', 123456, 15408);
    position = 1960751;
    await recording.onData(second, 'B', 124416, 15409);
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
    await recording.onData(first, 'B', 125376, 15410);
    assert.equal(published[2].metadata.relativeTimeMs, 40888);
    await recording.onConnectionReady();
    await recording.onData(first, 'B', 99, 5);
    assert.equal(published[3].metadata.relativeTimeMs, 50000);
  } finally {
    process.hrtime = originalHrtime;
  }
});
