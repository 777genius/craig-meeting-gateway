import assert from 'node:assert/strict';
import test from 'node:test';

import { MeetingParticipantLifecycle } from './meetingParticipantLifecycle';

const botId = '1533224474609057793';
const participantId = '1533227577286852649';

test('folds a participant join during startup into meeting.started without an early delta', () => {
  const lifecycle = new MeetingParticipantLifecycle();

  assert.equal(lifecycle.observe(participantId, true), null);
  assert.deepEqual(lifecycle.begin([], botId), [participantId]);
  assert.equal(lifecycle.observe(participantId, false), 'participant.left');
});

test('applies pre-start presence over a stale channel snapshot and emits later deltas once', () => {
  const lifecycle = new MeetingParticipantLifecycle();

  assert.equal(lifecycle.observe(participantId, false), null);
  assert.deepEqual(lifecycle.begin([botId, participantId], botId), []);
  assert.equal(lifecycle.observe(participantId, true), 'participant.joined');
  assert.equal(lifecycle.observe(participantId, true), null);
});
