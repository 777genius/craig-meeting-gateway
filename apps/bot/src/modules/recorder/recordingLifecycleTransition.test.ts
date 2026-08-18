import assert from 'node:assert/strict';
import Module from 'node:module';
import test from 'node:test';

import type { MeetingLifecycleEvent, MeetingLifecyclePublishOutcome } from './meetingIntegration';
import {
  type CraigLifecycleV3Admission,
  actorSemanticsVersion,
  createCraigLifecycleV3Producer,
  sealedActorRosterCapabilityId
} from './meetingLifecycleV3';
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
  const admissions: Array<CraigLifecycleV3Admission | undefined> = [];
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
        publishLifecycle(event: MeetingLifecycleEvent, admission?: CraigLifecycleV3Admission): MeetingLifecyclePublishOutcome {
          events.push(event);
          admissions.push(admission);
          return outcomes.shift() ?? { status: 'accepted' };
        }
      }
    }
  });

  return { recording, events, admissions };
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

test('the Recording Discord adapter derives fail-closed bot, system, and webhook actor evidence', async () => {
  for (const [signals, expectedKind] of [
    [{ bot: true }, 'automation'],
    [{ system: true }, 'automation'],
    [{ webhookID: '1533230920645308428' }, 'automation'],
    [{ bot: false, system: false, webhookID: null }, 'human']
  ] as const) {
    const { recording, events } = await createRecordingHarness({ status: 'accepted' });
    Object.assign(recording, {
      lifecycleV3: createCraigLifecycleV3Producer(
        {
          schemaVersion: 3,
          actorSemanticsVersion,
          producerCapabilityId: sealedActorRosterCapabilityId,
          producerRevision: '0123456789abcdef0123456789abcdef01234567'
        },
        { recordingId: 'recording-1', guildId: '1533232836297011436', channelId }
      )
    });
    await recording.onVoiceStateUpdate(
      { id: participantId, ...signals, voiceState: { channelID: channelId } } as Parameters<RecordingType['onVoiceStateUpdate']>[0],
      oldVoiceState
    );
    assert.equal(events[0]?.schemaVersion, 3);
    assert.deepEqual((events[0] as any).actor, { actorId: participantId, kind: expectedKind });
  }
});

test('rolls back rejected v3 producer evidence before the next accepted transition', async () => {
  const { recording, events, admissions } = await createRecordingHarness({ status: 'capacity-exhausted' }, { status: 'accepted' });
  const lifecycle = createCraigLifecycleV3Producer(
    {
      schemaVersion: 3,
      actorSemanticsVersion,
      producerCapabilityId: sealedActorRosterCapabilityId,
      producerRevision: '0123456789abcdef0123456789abcdef01234567'
    },
    { recordingId: 'recording-1', guildId: '1533232836297011436', channelId }
  );
  const started = lifecycle.started(
    {
      eventId: 'recording-1:started',
      recordingId: 'recording-1',
      guildId: '1533232836297011436',
      channelId,
      occurredAt: '2026-08-18T00:00:00.000Z'
    },
    [{ id: participantId, bot: false, system: false, webhook: false }]
  );
  const beforeRejectedTransition = lifecycle.durableSnapshot();
  lifecycle.durableAdmission = () => {
    throw new Error('participant transition materialized durable admission');
  };
  Object.assign(recording, { lifecycleV3: lifecycle });

  await recording.onVoiceStateUpdate({ ...participantMember(true), bot: true } as Parameters<RecordingType['onVoiceStateUpdate']>[0], oldVoiceState);
  assert.equal(admissions[0], undefined);
  assert.deepEqual(lifecycle.durableSnapshot(), beforeRejectedTransition);

  await recording.onVoiceStateUpdate(participantMember(true), oldVoiceState);
  const afterAcceptedTransition = lifecycle.durableSnapshot();

  assert.deepEqual(
    events.map(({ eventId }) => eventId),
    ['recording-1:1', 'recording-1:2']
  );
  assert.deepEqual(
    events.map((event) => (event.schemaVersion === 3 ? event.actorObservationState : null)),
    ['conflicted', 'consistent']
  );
  assert.equal(admissions[1], undefined);
  assert.deepEqual(afterAcceptedTransition.producer, beforeRejectedTransition.producer);
  assert.equal(afterAcceptedTransition.actorObservationState, 'consistent');
  assert.deepEqual(afterAcceptedTransition.actors, [{ actorId: participantId, kind: 'human' }]);
  assert.deepEqual(
    afterAcceptedTransition.pendingOutbox.map(({ eventId }) => eventId),
    [started.eventId, 'recording-1:2']
  );
});

test('admits a transition without materializing growing producer history', async () => {
  const { recording, events } = await createRecordingHarness({ status: 'accepted' });
  const lifecycle = createCraigLifecycleV3Producer(
    {
      schemaVersion: 3,
      actorSemanticsVersion,
      producerCapabilityId: sealedActorRosterCapabilityId,
      producerRevision: '0123456789abcdef0123456789abcdef01234567'
    },
    { recordingId: 'recording-1', guildId: '1533232836297011436', channelId }
  );
  lifecycle.started(
    { eventId: 'history:0', recordingId: 'recording-1', guildId: '1533232836297011436', channelId, occurredAt: '2026-08-18T00:00:00.000Z' },
    []
  );
  for (let index = 1; index <= 500; index++)
    lifecycle.connection(
      { eventId: `history:${index}`, recordingId: 'recording-1', guildId: '1533232836297011436', channelId, occurredAt: `2026-08-18T00:${String(Math.floor(index / 60)).padStart(2, '0')}:${String(index % 60).padStart(2, '0')}.000Z` },
      index % 2 === 0 ? 'meeting.connection_recovered' : 'meeting.connection_lost',
      null
    );
  lifecycle.durableSnapshot = () => { throw new Error('full history snapshot created during admission'); };
  Object.assign(recording, { lifecycleV3: lifecycle });

  await recording.onVoiceStateUpdate(participantMember(true), oldVoiceState);
  assert.equal(events.at(-1)?.type, 'participant.joined');
});
