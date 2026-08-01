import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  MeetingAutoRecordActiveRecording,
  MeetingAutoRecordChannel,
  MeetingAutoRecordCoordinator,
  MeetingAutoRecordPort,
  normalizeMeetingAutoRecordConfig
} from './meetingAutoRecordCoordinator';

const guildId = '1533228590643155034';
const channelId = '1533228823045214398';
const selfId = '1533224474609057793';
const humanId = '1533227577286852649';
const syntheticBotId = '1533228054724346087';

class FakePort implements MeetingAutoRecordPort {
  channel: MeetingAutoRecordChannel = { id: channelId, guildId, participants: [] };
  active?: MeetingAutoRecordActiveRecording;
  starts: string[] = [];
  stops: string[] = [];

  getChannel(requestGuildId: string, requestChannelId: string) {
    return requestGuildId === guildId && requestChannelId === channelId ? this.channel : undefined;
  }

  getActiveRecording() {
    return this.active;
  }

  async start(_: string, requestedChannelId: string, ownerId: string) {
    this.starts.push(ownerId);
    this.active = { id: 'recording-1', channelId: requestedChannelId };
    return this.active;
  }

  async stop(_: string, recordingId: string) {
    this.stops.push(recordingId);
    this.active = undefined;
  }
}

function createCoordinator(port: FakePort, syntheticBotUserIds: string[] = []) {
  return new MeetingAutoRecordCoordinator(
    normalizeMeetingAutoRecordConfig({ enabled: true, channelIds: [channelId], syntheticBotUserIds }),
    selfId,
    port
  );
}

test('disabled configuration is inert and does not require allowlists', async () => {
  const port = new FakePort();
  port.channel.participants = [{ id: humanId, bot: false }];
  const coordinator = new MeetingAutoRecordCoordinator(normalizeMeetingAutoRecordConfig({ enabled: false }), selfId, port);

  await coordinator.reconcile(guildId, channelId);

  assert.deepEqual(port.starts, []);
});

test('starts once for the first human under concurrent membership events', async () => {
  const port = new FakePort();
  port.channel.participants = [
    { id: selfId, bot: true },
    { id: humanId, bot: false }
  ];
  const coordinator = createCoordinator(port);

  await Promise.all([coordinator.reconcile(guildId, channelId), coordinator.reconcile(guildId, channelId)]);

  assert.deepEqual(port.starts, [humanId]);
});

test('ignores ordinary bots and Craig but accepts explicitly allowlisted synthetic bots', async () => {
  const port = new FakePort();
  const coordinator = createCoordinator(port, [syntheticBotId]);
  port.channel.participants = [
    { id: selfId, bot: true },
    { id: '1533228054724346088', bot: true }
  ];

  await coordinator.reconcile(guildId, channelId);
  assert.deepEqual(port.starts, []);

  port.channel.participants.push({ id: syntheticBotId, bot: true });
  await coordinator.reconcile(guildId, channelId);
  assert.deepEqual(port.starts, [syntheticBotId]);
});

test('prefers a human owner when a synthetic bot is already present', async () => {
  const port = new FakePort();
  port.channel.participants = [
    { id: syntheticBotId, bot: true },
    { id: humanId, bot: false }
  ];
  const coordinator = createCoordinator(port, [syntheticBotId]);

  await coordinator.reconcile(guildId, channelId);

  assert.deepEqual(port.starts, [humanId]);
});

test('stops only its own recording after all eligible participants leave', async () => {
  const port = new FakePort();
  port.channel.participants = [{ id: humanId, bot: false }];
  const coordinator = createCoordinator(port);
  await coordinator.reconcile(guildId, channelId);

  port.channel.participants = [{ id: selfId, bot: true }];
  await coordinator.reconcile(guildId, channelId);

  assert.deepEqual(port.stops, ['recording-1']);
});

test('never stops or replaces a manual recording', async () => {
  const port = new FakePort();
  port.active = { id: 'manual-recording', channelId };
  const coordinator = createCoordinator(port);

  await coordinator.reconcile(guildId, channelId);

  assert.deepEqual(port.starts, []);
  assert.deepEqual(port.stops, []);
  assert.equal(port.active.id, 'manual-recording');
});

test('does not stop during a transient leave and rejoin inside the empty grace period', async () => {
  const port = new FakePort();
  port.channel.participants = [{ id: humanId, bot: false }];
  const coordinator = new MeetingAutoRecordCoordinator(
    normalizeMeetingAutoRecordConfig({
      enabled: true,
      channelIds: [channelId],
      startDelayMs: 0,
      emptyGraceMs: 30
    }),
    selfId,
    port
  );
  await coordinator.reconcile(guildId, channelId);

  port.channel.participants = [];
  coordinator.schedule(guildId, channelId, true);
  port.channel.participants = [{ id: humanId, bot: false }];
  coordinator.schedule(guildId, channelId, false);
  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.deepEqual(port.stops, []);
  assert.equal(port.active?.id, 'recording-1');
  coordinator.dispose();
});

test('configuration fails closed for empty, invalid, or oversized allowlists', () => {
  assert.throws(() => normalizeMeetingAutoRecordConfig({ enabled: true }), /at least one channel/);
  assert.throws(() => normalizeMeetingAutoRecordConfig({ enabled: true, channelIds: ['not-a-snowflake'] }), /Discord snowflake/);
  assert.throws(
    () => normalizeMeetingAutoRecordConfig({ enabled: true, channelIds: [channelId], syntheticBotUserIds: Array(129).fill(syntheticBotId) }),
    /more than 128/
  );
  assert.throws(() => normalizeMeetingAutoRecordConfig({ enabled: true, channelIds: [channelId], emptyGraceMs: 60_001 }), /between 0 and 60000/);
});
