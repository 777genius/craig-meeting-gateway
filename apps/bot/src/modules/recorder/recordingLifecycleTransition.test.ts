import assert from 'node:assert/strict';
import Module from 'node:module';
import test from 'node:test';

import type { MeetingLifecycleEvent, MeetingLifecyclePublishOutcome } from './meetingIntegration';
import { MeetingParticipantLifecycle } from './meetingParticipantLifecycle';
import type RecordingType from './recording';

type ModuleLoader = (request: string, parent: unknown, isMain: boolean) => unknown;

let Recording: typeof RecordingType;

const recordingLoaded = (async () => {
  const loader = Module as unknown as { _load: ModuleLoader };
  const originalLoad = loader._load;
  loader._load = (request, parent, isMain) => {
    if (request === '@discordjs/opus') return { OpusEncoder: class {} };
    if (request === '../../influx') return { onRecordingEnd() {}, onRecordingStart() {} };
    if (request === '../../prisma') return { prisma: {} };
    if (request === '../../util') return { getSelfMember() {}, wait: async () => {} };
    return originalLoad(request, parent, isMain);
  };

  try {
    ({ default: Recording } = await import('./recording'));
  } finally {
    loader._load = originalLoad;
  }
})();

const botId = '1533224474609057793';
const participantId = '1533227577286852649';
const channelId = '1533230920645308427';

interface RecordingLifecycleInternals {
  markConnectionLost(reason: string): void;
  markConnectionRecovered(): void;
}

async function createRecordingHarness(...outcomes: MeetingLifecyclePublishOutcome[]) {
  await recordingLoaded;
  const events: MeetingLifecycleEvent[] = [];
  const participants = new MeetingParticipantLifecycle();
  participants.begin([], botId);

  const recording = Object.create(Recording.prototype) as RecordingType;
  Object.assign(recording, {
    id: 'recording-1',
    lifecycleSequence: 0,
    meetingParticipants: participants,
    connectionLossOpen: false,
    channel: {
      id: channelId,
      guild: { id: '1533232836297011436' }
    },
    recorder: {
      client: { bot: { user: { id: botId } } },
      logger: {
        debug() {},
        error() {},
        warn() {}
      },
      meetingIntegration: {
        publishLifecycle(event: MeetingLifecycleEvent): MeetingLifecyclePublishOutcome {
          events.push(event);
          return outcomes.shift() ?? { status: 'accepted' };
        }
      }
    }
  });

  return { recording, events };
}

function participantMember(isPresent: boolean): Parameters<RecordingType['onVoiceStateUpdate']>[0] {
  return {
    id: participantId,
    voiceState: { channelID: isPresent ? channelId : null }
  } as Parameters<RecordingType['onVoiceStateUpdate']>[0];
}

const oldVoiceState = {} as Parameters<RecordingType['onVoiceStateUpdate']>[1];

test('does not emit participant.left after capacity rejects participant.joined', async () => {
  const { recording, events } = await createRecordingHarness({ status: 'capacity-exhausted' }, { status: 'accepted' }, { status: 'accepted' });

  await recording.onVoiceStateUpdate(participantMember(true), oldVoiceState);
  await recording.onVoiceStateUpdate(participantMember(false), oldVoiceState);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['participant.joined']
  );

  await recording.onVoiceStateUpdate(participantMember(true), oldVoiceState);
  await recording.onVoiceStateUpdate(participantMember(false), oldVoiceState);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['participant.joined', 'participant.joined', 'participant.left']
  );
});

test('does not emit participant.joined after capacity rejects participant.left', async () => {
  const { recording, events } = await createRecordingHarness(
    { status: 'accepted' },
    { status: 'capacity-exhausted' },
    { status: 'accepted' },
    { status: 'accepted' }
  );

  await recording.onVoiceStateUpdate(participantMember(true), oldVoiceState);
  await recording.onVoiceStateUpdate(participantMember(false), oldVoiceState);
  await recording.onVoiceStateUpdate(participantMember(true), oldVoiceState);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['participant.joined', 'participant.left']
  );

  await recording.onVoiceStateUpdate(participantMember(false), oldVoiceState);
  await recording.onVoiceStateUpdate(participantMember(true), oldVoiceState);
  assert.deepEqual(
    events.map(({ type }) => type),
    ['participant.joined', 'participant.left', 'participant.left', 'participant.joined']
  );
});

test('suppresses recovery after a dropped connection loss and accepts the next complete pair', async () => {
  const { recording, events } = await createRecordingHarness({ status: 'capacity-exhausted' }, { status: 'accepted' }, { status: 'accepted' });
  const transitions = recording as unknown as RecordingLifecycleInternals;

  transitions.markConnectionLost('first disconnect');
  transitions.markConnectionRecovered();
  assert.deepEqual(
    events.map(({ type }) => type),
    ['meeting.connection_lost']
  );

  transitions.markConnectionLost('second disconnect');
  transitions.markConnectionRecovered();
  assert.deepEqual(
    events.map(({ type }) => type),
    ['meeting.connection_lost', 'meeting.connection_lost', 'meeting.connection_recovered']
  );
});

test('keeps a loss open when capacity rejects recovery', async () => {
  const { recording, events } = await createRecordingHarness({ status: 'accepted' }, { status: 'capacity-exhausted' }, { status: 'accepted' });
  const transitions = recording as unknown as RecordingLifecycleInternals;

  transitions.markConnectionLost('disconnect');
  transitions.markConnectionRecovered();
  transitions.markConnectionLost('duplicate disconnect');
  transitions.markConnectionRecovered();

  assert.deepEqual(
    events.map(({ type }) => type),
    ['meeting.connection_lost', 'meeting.connection_recovered', 'meeting.connection_recovered']
  );
});
