export const sealedActorRosterCapabilityId = 'meeting.lifecycle.sealed-actor-roster.v1' as const;
export const actorSemanticsVersion = 1 as const;

const discordSnowflake = /^\d{17,20}$/;
const immutableRevision = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

export type CraigActorKind = 'human' | 'automation' | 'unknown';
export interface CraigActor {
  actorId: string;
  kind: CraigActorKind;
}
export interface CraigProducerIdentity {
  actorSemanticsVersion: typeof actorSemanticsVersion;
  producerCapabilityId: typeof sealedActorRosterCapabilityId;
  producerRevision: string;
}
export type MeetingLifecycleProducerConfiguration = Readonly<{ schemaVersion: 1 }> | Readonly<{ schemaVersion: 3 } & CraigProducerIdentity>;
export interface CraigLifecycleEnvelope {
  eventId: string;
  recordingId: string;
  guildId: string;
  channelId: string;
  occurredAt: string;
}
interface CraigLifecycleV3Envelope extends CraigLifecycleEnvelope, CraigProducerIdentity {
  schemaVersion: 3;
  actorObservationState: 'consistent' | 'conflicted';
}
export type CraigLifecycleV3Event =
  | (CraigLifecycleV3Envelope & { type: 'meeting.started'; actors: CraigActor[]; rosterState: 'unsealed' })
  | (CraigLifecycleV3Envelope & { type: 'participant.joined' | 'participant.left'; actor: CraigActor })
  | (CraigLifecycleV3Envelope & {
      type: 'meeting.connection_lost' | 'meeting.connection_recovered' | 'meeting.ended' | 'meeting.aborted';
      reason: string | null;
    })
  | (CraigLifecycleV3Envelope & {
      type: 'recording.authoritative_ready';
      actors: CraigActor[];
      rosterState: 'sealed';
      endedAt: string;
      trackCount: number;
      sourceFilesChecksumSha256: string;
    });
export interface DurableActorObservationSnapshot {
  schemaVersion: 1;
  producer: CraigProducerIdentity;
  actorObservationState: 'consistent' | 'conflicted';
  actors: CraigActor[];
}

/** Producer-owned evidence that is safe to persist inside the recording outbox. */
export class CraigActorObservationLedger {
  private readonly kindsByActor = new Map<string, CraigActorKind>();
  private conflicted = false;

  constructor(readonly producer: CraigProducerIdentity) {
    assertProducerIdentity(producer);
  }

  observe(actor: CraigActor): void {
    assertActor(actor);
    const existing = this.kindsByActor.get(actor.actorId);
    if (existing === undefined && this.kindsByActor.size >= 1_000) throw new Error('Craig actor roster exceeds its bounded size');
    if (existing !== undefined && existing !== actor.kind) this.conflicted = true;
    else this.kindsByActor.set(actor.actorId, actor.kind);
  }

  observationState(): 'consistent' | 'conflicted' {
    return this.conflicted ? 'conflicted' : 'consistent';
  }
  actors(): CraigActor[] {
    return [...this.kindsByActor].sort(([left], [right]) => left.localeCompare(right)).map(([actorId, kind]) => ({ actorId, kind }));
  }
  snapshot(): DurableActorObservationSnapshot {
    return {
      schemaVersion: 1,
      producer: { ...this.producer },
      actorObservationState: this.observationState(),
      actors: this.actors()
    };
  }

  static restore(value: unknown, expectedProducer: CraigProducerIdentity): CraigActorObservationLedger {
    assertProducerIdentity(expectedProducer);
    if (!isRecord(value) || !hasExactlyKeys(value, ['schemaVersion', 'producer', 'actorObservationState', 'actors']))
      throw new Error('Durable actor observation snapshot is malformed');
    if (value.schemaVersion !== 1 || !isRecord(value.producer) || !Array.isArray(value.actors))
      throw new Error('Durable actor observation snapshot has an unsupported version');
    if (!hasExactlyKeys(value.producer, ['actorSemanticsVersion', 'producerCapabilityId', 'producerRevision']))
      throw new Error('Durable actor observation snapshot producer is malformed');
    const producer = parseProducerIdentity(value.producer);
    if (!sameProducer(producer, expectedProducer)) throw new Error('Durable actor observation snapshot belongs to another producer revision');
    if (value.actorObservationState !== 'consistent' && value.actorObservationState !== 'conflicted')
      throw new Error('Durable actor observation snapshot state is invalid');
    const ledger = new CraigActorObservationLedger(producer);
    for (const actor of value.actors) {
      if (!isRecord(actor)) throw new Error('Durable actor observation snapshot contains an invalid actor');
      ledger.observe(parseActor(actor));
    }
    if (value.actorObservationState === 'conflicted') ledger.conflicted = true;
    else if (ledger.conflicted) throw new Error('Durable actor observation snapshot hides a conflict');
    return ledger;
  }
}

export class CraigLifecycleV3Producer {
  constructor(private readonly ledger: CraigActorObservationLedger) {}

  started(envelope: CraigLifecycleEnvelope, actors: readonly CraigActor[]): CraigLifecycleV3Event {
    for (const actor of actors) this.ledger.observe(actor);
    return { ...this.envelope(envelope), type: 'meeting.started', actors: this.ledger.actors(), rosterState: 'unsealed' };
  }
  participant(envelope: CraigLifecycleEnvelope, type: 'participant.joined' | 'participant.left', actor: CraigActor): CraigLifecycleV3Event {
    this.ledger.observe(actor);
    return { ...this.envelope(envelope), type, actor: { ...actor } };
  }
  connection(
    envelope: CraigLifecycleEnvelope,
    type: 'meeting.connection_lost' | 'meeting.connection_recovered',
    reason: string | null
  ): CraigLifecycleV3Event {
    assertReason(reason);
    return { ...this.envelope(envelope), type, reason };
  }
  terminal(envelope: CraigLifecycleEnvelope, type: 'meeting.ended' | 'meeting.aborted', reason: string | null): CraigLifecycleV3Event {
    assertReason(reason);
    return { ...this.envelope(envelope), type, reason };
  }
  authoritativeReady(
    envelope: CraigLifecycleEnvelope,
    input: Readonly<{ actors: readonly CraigActor[]; endedAt: string; sourceFilesChecksumSha256: string; trackCount: number }>
  ): CraigLifecycleV3Event {
    assertAuthoritativeReady(input);
    for (const actor of input.actors) this.ledger.observe(actor);
    return {
      ...this.envelope(envelope),
      type: 'recording.authoritative_ready',
      actors: this.ledger.actors(),
      rosterState: 'sealed',
      endedAt: input.endedAt,
      sourceFilesChecksumSha256: input.sourceFilesChecksumSha256,
      trackCount: input.trackCount
    };
  }
  durableSnapshot(): DurableActorObservationSnapshot {
    return this.ledger.snapshot();
  }
  private envelope(value: CraigLifecycleEnvelope): CraigLifecycleV3Envelope {
    assertEnvelope(value);
    return { ...value, schemaVersion: 3, ...this.ledger.producer, actorObservationState: this.ledger.observationState() };
  }
}

export function parseMeetingLifecycleProducerConfiguration(value: unknown): MeetingLifecycleProducerConfiguration {
  if (!isRecord(value) || typeof value.schemaVersion !== 'number') throw new Error('Meeting lifecycle producer configuration is malformed');
  if (value.schemaVersion === 1) {
    if (!hasExactlyKeys(value, ['schemaVersion'])) throw new Error('Legacy producer configuration cannot claim v3 capabilities');
    return { schemaVersion: 1 };
  }
  if (value.schemaVersion !== 3 || !hasExactlyKeys(value, ['schemaVersion', 'actorSemanticsVersion', 'producerCapabilityId', 'producerRevision']))
    throw new Error('Meeting lifecycle producer configuration has an unsupported version');
  return { schemaVersion: 3, ...parseProducerIdentity(value) };
}

export function createCraigLifecycleV3Producer(
  config: Extract<MeetingLifecycleProducerConfiguration, { schemaVersion: 3 }>,
  snapshot?: unknown
): CraigLifecycleV3Producer {
  const producer = parseProducerIdentity(config);
  const ledger = snapshot === undefined ? new CraigActorObservationLedger(producer) : CraigActorObservationLedger.restore(snapshot, producer);
  return new CraigLifecycleV3Producer(ledger);
}

function parseProducerIdentity(value: Record<string, unknown>): CraigProducerIdentity {
  const producer: CraigProducerIdentity = {
    actorSemanticsVersion: value.actorSemanticsVersion as typeof actorSemanticsVersion,
    producerCapabilityId: value.producerCapabilityId as typeof sealedActorRosterCapabilityId,
    producerRevision: value.producerRevision as string
  };
  assertProducerIdentity(producer);
  return producer;
}
function assertProducerIdentity(value: CraigProducerIdentity): void {
  if (
    value.actorSemanticsVersion !== actorSemanticsVersion ||
    value.producerCapabilityId !== sealedActorRosterCapabilityId ||
    typeof value.producerRevision !== 'string' ||
    !immutableRevision.test(value.producerRevision)
  )
    throw new Error('Craig producer identity is invalid');
}
function sameProducer(left: CraigProducerIdentity, right: CraigProducerIdentity): boolean {
  return (
    left.actorSemanticsVersion === right.actorSemanticsVersion &&
    left.producerCapabilityId === right.producerCapabilityId &&
    left.producerRevision === right.producerRevision
  );
}
function parseActor(value: Record<string, unknown>): CraigActor {
  const actor = { actorId: value.actorId as string, kind: value.kind as CraigActorKind };
  assertActor(actor);
  if (!hasExactlyKeys(value, ['actorId', 'kind'])) throw new Error('Craig actor contains unknown fields');
  return actor;
}
function assertActor(value: CraigActor): void {
  if (!discordSnowflake.test(value.actorId) || !['human', 'automation', 'unknown'].includes(value.kind)) throw new Error('Craig actor is invalid');
}
function assertReason(value: string | null): void {
  if (value !== null && (value.length < 1 || value.length > 256)) throw new Error('Craig lifecycle reason is invalid');
}
function assertAuthoritativeReady(value: Readonly<{ endedAt: string; sourceFilesChecksumSha256: string; trackCount: number }>): void {
  const endedAt = Date.parse(value.endedAt);
  if (
    !Number.isFinite(endedAt) ||
    new Date(endedAt).toISOString() !== value.endedAt ||
    !/^[0-9a-f]{64}$/.test(value.sourceFilesChecksumSha256) ||
    !Number.isSafeInteger(value.trackCount) ||
    value.trackCount < 1 ||
    value.trackCount > 64
  )
    throw new Error('Craig authoritative-ready evidence is invalid');
}

function assertEnvelope(value: CraigLifecycleEnvelope): void {
  const parsedAt = Date.parse(value.occurredAt);
  if (
    value.eventId.length < 1 ||
    value.eventId.length > 128 ||
    value.recordingId.length < 1 ||
    value.recordingId.length > 128 ||
    !discordSnowflake.test(value.guildId) ||
    !discordSnowflake.test(value.channelId) ||
    !Number.isFinite(parsedAt) ||
    new Date(parsedAt).toISOString() !== value.occurredAt
  )
    throw new Error('Craig lifecycle envelope is invalid');
}
function hasExactlyKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  return Object.keys(value).length === keys.length && keys.every((key) => key in value);
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
