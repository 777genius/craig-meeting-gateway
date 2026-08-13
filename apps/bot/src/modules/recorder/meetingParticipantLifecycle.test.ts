import assert from 'node:assert/strict';
import test from 'node:test';

import { MeetingParticipantLifecycle } from './meetingParticipantLifecycle';

const participantId = '1533227577286852649';

test('folds a participant join during startup into meeting.started without an early delta', () => {
  const lifecycle = new MeetingParticipantLifecycle();

  assert.equal(lifecycle.observe(participantId, true), null);
  assert.deepEqual(lifecycle.begin([]), [participantId]);
  assert.equal(lifecycle.observe(participantId, false), 'participant.left');
});

test('applies pre-start presence over a stale channel snapshot and emits later deltas once', () => {
  const lifecycle = new MeetingParticipantLifecycle();

  assert.equal(lifecycle.observe(participantId, false), null);
  assert.deepEqual(lifecycle.begin([participantId]), []);
  assert.equal(lifecycle.observe(participantId, true), 'participant.joined');
  assert.equal(lifecycle.observe(participantId, true), null);
});
