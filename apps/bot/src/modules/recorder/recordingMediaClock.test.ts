import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RecordingMediaClock } from './recordingMediaClock';

test('RTP sequences 15408/15409 advance 20ms despite exact bunched Craig arrivals', () => {
  const clock = new RecordingMediaClock();
  const observations = [
    { sequence: 15408, timestamp: 123456, arrival: 1960732 },
    { sequence: 15409, timestamp: 124416, arrival: 1960751 }
  ];
  assert.deepEqual(observations.map(({ arrival }) => Math.trunc(arrival / 48)), [40848, 40848]);
  assert.deepEqual(observations.map(({ timestamp, arrival }) => clock.map('B', timestamp, arrival)), [40848, 40868]);
});

test('wrap and real missing-media gaps preserve RTP progression, independent of arrival jitter', () => {
  const clock = new RecordingMediaClock();
  assert.equal(clock.map('A', 0xfffffe20, 48000), 1000);
  assert.equal(clock.map('A', 480, 48001), 1020);
  assert.equal(clock.map('A', 480 + 960 * 22, 48002), 1460);
  assert.equal(clock.map('B', 100, 96000), 2000);
  assert.equal(clock.map('A', 480 + 960 * 23, 150000), 1480);
});

test('explicit reconnect epoch reanchors each speaker to the continuing recording origin', () => {
  const clock = new RecordingMediaClock();
  clock.map('A', 123456, 48000);
  clock.map('B', 654321, 96000);
  // A resumed connection keeps its RTP epoch, even with an arrival delay.
  assert.equal(clock.map('A', 124416, 240000), 1020);
  clock.beginEpoch();
  assert.equal(clock.map('A', 50, 480000), 10000);
  assert.equal(clock.map('A', 1010, 480001), 10020);
  assert.equal(clock.map('B', 99, 528000), 11000);
});

test('duplicates and reordered timestamps remain visible, without artificial monotonic clamping', () => {
  const clock = new RecordingMediaClock();
  assert.equal(clock.map('A', 1000, 48000), 1000);
  assert.equal(clock.map('A', 1960, 48001), 1020);
  assert.equal(clock.map('A', 1960, 49000), 1020);
  assert.equal(clock.map('A', 1000, 50000), 1000);
  assert.equal(clock.map('A', 2920, 51000), 1040);
});
