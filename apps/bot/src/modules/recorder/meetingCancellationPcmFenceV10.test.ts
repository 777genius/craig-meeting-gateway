import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { test } from 'node:test';

import {
  type CancellationPcmFenceCollector,
  type CancellationPcmFenceDurability,
  type DurableBotikTrackProof,
  MeetingCancellationPcmFenceV10,
  cancellationReceiptDigest,
  createAuthoritativeCancellationPcmFenceLog
} from './meetingCancellationPcmFenceV10';

const track = Object.freeze({ speakerId: '1533228054724346087', trackNumber: 7, packetCursor: 8 });
const proof: DurableBotikTrackProof = Object.freeze({ track, checksumSha256: 'a'.repeat(64), sizeBytes: 4096 });
const at = (second: number) => `2026-08-18T00:00:${String(second).padStart(2, '0')}.000Z`;

function fixture(overrides: Partial<CancellationPcmFenceDurability> = {}) {
  const order: string[] = [];
  const events: unknown[] = [];
  const snapshots: unknown[] = [];
  const durability: CancellationPcmFenceDurability = {
    flushAndChecksum: async () => {
      order.push('flush');
      return proof;
    },
    persist: async (snapshot) => {
      order.push('persist');
      snapshots.push(snapshot);
    },
    ...overrides
  };
  const collector: CancellationPcmFenceCollector = {
    emit: (event) => {
      order.push('emit');
      events.push(event);
    }
  };
  const fence = new MeetingCancellationPcmFenceV10('meeting-1', 'turn-1', 3, track, durability, collector);
  return { events, fence, order, snapshots };
}

test('emits the exact receipt only after durable flush/checksum and snapshot persistence', async () => {
  const { events, fence, order, snapshots } = fixture();
  assert.equal(fence.acceptFactualPcm(0, at(1)), 'accepted');
  assert.equal(fence.acceptFactualPcm(1, at(2)), 'accepted');
  assert.equal(fence.cancel('barge-in', at(3)), 'accepted');
  const receipt = await fence.finalize();
  assert.deepEqual(order, ['flush', 'persist', 'emit']);
  assert.equal(events[0], receipt);
  assert.equal((snapshots[0] as { receipt: unknown }).receipt, receipt);
  assert.deepEqual(receipt, {
    schemaVersion: 10,
    type: 'meeting.playback_cancelled',
    capabilityId: 'meeting.cancellation.pcm-fence.v10',
    meetingId: 'meeting-1',
    turnId: 'turn-1',
    cancelReason: 'barge-in',
    cancelledAt: at(3),
    lastAcceptedFactualPcmSequence: 1,
    lastAcceptedFactualPcmAt: at(2),
    noFactualPcmAcceptedAfterFence: true,
    botikTrack: track,
    botikTrackChecksumSha256: 'a'.repeat(64),
    botikTrackSizeBytes: 4096,
    playbackGeneration: 3
  });
  assert.match(cancellationReceiptDigest(receipt), /^[a-f0-9]{64}$/);
});

test('rejects late PCM and makes identical cancellation/finalize retries idempotent', async () => {
  const { events, fence, order } = fixture();
  assert.equal(fence.acceptFactualPcm(0, at(1)), 'accepted');
  assert.equal(fence.acceptFactualPcm(0, at(1)), 'duplicate');
  assert.equal(fence.cancel('disconnect', at(2)), 'accepted');
  assert.equal(fence.cancel('disconnect', at(2)), 'duplicate');
  assert.equal(fence.acceptFactualPcm(1, at(3)), 'late');
  const [first, second] = await Promise.all([fence.finalize(), fence.finalize()]);
  assert.equal(first, second);
  assert.equal(await fence.finalize(), first);
  assert.equal(events.length, 1);
  assert.deepEqual(order, ['flush', 'persist', 'emit']);
});

test('rejects gaps, reordered time, and conflicting retries without moving the boundary', () => {
  const { fence } = fixture();
  assert.equal(fence.acceptFactualPcm(1, at(1)), 'conflict');
  assert.equal(fence.acceptFactualPcm(0, at(2)), 'accepted');
  assert.equal(fence.acceptFactualPcm(0, at(3)), 'conflict');
  assert.equal(fence.acceptFactualPcm(1, at(1)), 'conflict');
  assert.equal(fence.cancel('meeting-ended', at(3)), 'accepted');
  assert.equal(fence.cancel('disconnect', at(3)), 'conflict');
});

test('rejects expanded-year timestamps before durable proof work', () => {
  const { fence, order } = fixture();
  assert.throws(
    () => fence.acceptFactualPcm(0, '+010000-01-01T00:00:00.000Z'),
    /canonical timestamp/
  );
  assert.throws(
    () => fence.cancel('disconnect', '+010000-01-01T00:00:00.000Z'),
    /canonical timestamp/
  );
  assert.deepEqual(order, []);
  assert.equal(fence.snapshot().fence, null);
});

for (const failure of ['unknown', 'interrupted', 'failed', 'lost'] as const) {
  test(`fails closed when the durable flush is ${failure}`, async () => {
    const { events, fence, order, snapshots } = fixture({
      flushAndChecksum: async () => {
        order.push('flush');
        throw new Error(failure);
      }
    });
    fence.cancel('disconnect', at(1));
    await assert.rejects(fence.finalize(), new RegExp(failure));
    assert.deepEqual(order, ['flush']);
    assert.deepEqual(events, []);
    assert.deepEqual(snapshots, []);
  });
}

test('fails closed on invalid proof and persistence failure', async () => {
  const invalid = fixture({ flushAndChecksum: async () => ({ ...proof, checksumSha256: 'unknown' }) });
  invalid.fence.cancel('disconnect', at(1));
  await assert.rejects(invalid.fence.finalize(), /checksum/);
  assert.deepEqual(invalid.events, []);

  const persistence = fixture({ persist: async () => Promise.reject(new Error('disk lost')) });
  persistence.fence.cancel('disconnect', at(1));
  await assert.rejects(persistence.fence.finalize(), /disk lost/);
  assert.deepEqual(persistence.order, ['flush']);
  assert.deepEqual(persistence.events, []);
});

test('restores a fenced pre-flush snapshot and safely completes recovery', async () => {
  const initial = fixture();
  initial.fence.acceptFactualPcm(0, at(1));
  initial.fence.cancel('meeting-ended', at(2));
  const recoveredFixture = fixture();
  const recovered = MeetingCancellationPcmFenceV10.restore(initial.fence.snapshot(), track, {
    flushAndChecksum: async () => proof,
    persist: async (snapshot) => recoveredFixture.snapshots.push(snapshot)
  }, { emit: (event) => recoveredFixture.events.push(event) });
  assert.equal(recovered.acceptFactualPcm(1, at(3)), 'late');
  const receipt = await recovered.finalize();
  assert.equal(receipt.playbackGeneration, 3);
  assert.equal(recoveredFixture.events.length, 1);
});

test('replays a durably persisted receipt after restart without reflushing', async () => {
  const initial = fixture();
  initial.fence.cancel('barge-in', at(1));
  const receipt = await initial.fence.finalize();
  const replayed: unknown[] = [];
  const recovered = MeetingCancellationPcmFenceV10.restore(initial.fence.snapshot(), track, {
    flushAndChecksum: async () => { throw new Error('must not flush'); },
    persist: async () => { throw new Error('must not persist'); }
  }, { emit: (event) => replayed.push(event) });
  assert.deepEqual(await recovered.finalize(), receipt);
  assert.deepEqual(replayed, [receipt]);
  assert.deepEqual(await recovered.finalize(), receipt);
  assert.equal(replayed.length, 1);
});

test('shared meeting-verifier fixture matches the final-upload proof adapter byte-for-byte', async () => {
  const root = path.resolve(__dirname, '../../../contracts/meeting-cancellation-pcm-fence-v10');
  const fixtureFile = JSON.parse(await readFile(path.join(root, 'canonical-fixture.json'), 'utf8')) as { proof: unknown };
  assert.deepEqual(
    createAuthoritativeCancellationPcmFenceLog({
      attemptedPacketCountAfterCancellation: 0,
      attemptId: 'attempt-1',
      cancellationObservedAt: at(3),
      fenceObservedAt: at(3),
      meetingId: 'meeting-1',
      recordingId: 'recording-1',
      trackSha256: 'a'.repeat(64),
      turnId: 'turn-1'
    }),
    fixtureFile.proof
  );
});
