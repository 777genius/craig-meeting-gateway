export const sealedActorRosterCapabilityId = 'meeting.lifecycle.sealed-actor-roster.v1' as const;
export const actorSemanticsVersion = 1 as const;
export const meetingLifecycleV3SchemaVersion = 3 as const;
export const maximumCraigActorRosterSize = 1_000 as const;
/** Hard fail-closed bound for one recording's unacknowledged durable journal. */
export const maximumCraigPendingLifecycleEvents = 1_024 as const;

const discordSnowflake = /^\d{17,20}$/;
const immutableRevision = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

export type CraigActorKind = 'human' | 'automation' | 'unknown';
export type CraigActor = Readonly<{ actorId: string; kind: CraigActorKind }>;

/** Signals populated by the authenticated Discord adapter, never by an application caller. */
export type AuthenticatedDiscordActor = Readonly<{
  id: string;
  bot?: boolean;
  system?: boolean;
  webhook?: boolean;
}>;

export type CraigProducerIdentity = Readonly<{
  actorSemanticsVersion: typeof actorSemanticsVersion;
  producerCapabilityId: typeof sealedActorRosterCapabilityId;
  producerRevision: string;
}>;

export type CraigLifecycleContext = Readonly<{ recordingId: string; guildId: string; channelId: string }>;
export type MeetingLifecycleProducerConfiguration = Readonly<{ schemaVersion: 1 }> | Readonly<{ schemaVersion: 3 } & CraigProducerIdentity>;
export type CraigLifecycleEnvelope = CraigLifecycleContext & Readonly<{ eventId: string; occurredAt: string }>;

interface CraigLifecycleV3Envelope extends CraigLifecycleEnvelope, CraigProducerIdentity {
  schemaVersion: typeof meetingLifecycleV3SchemaVersion;
  actorObservationState: 'consistent' | 'conflicted';
}

export type CraigLifecycleV3Event =
  | (CraigLifecycleV3Envelope & { type: 'meeting.started'; actors: CraigActor[]; rosterState: 'unsealed' })
  | (CraigLifecycleV3Envelope & { type: 'participant.joined' | 'participant.left'; actor: CraigActor })
  | (CraigLifecycleV3Envelope & {
      type: 'meeting.connection_lost' | 'meeting.connection_recovered';
      reason: string | null;
    })
  | (CraigLifecycleV3Envelope & {
      type: 'meeting.ended' | 'meeting.aborted';
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

export type DurableCraigLifecycleV3Snapshot = Readonly<{
  schemaVersion: 3;
  recordingId: string;
  guildId: string;
  channelId: string;
  producer: CraigProducerIdentity;
  actorObservationState: 'consistent' | 'conflicted';
  actors: CraigActor[];
  emitted: Array<Readonly<{ eventId: string; occurredAt: string; type: CraigLifecycleV3Event['type'] }>>;
  pendingOutbox: CraigLifecycleV3Event[];
  sealedReady: CraigLifecycleV3Event | null;
}>;

/** Bounded producer state accompanying one admission; it never copies event history. */
export type CraigLifecycleV3Admission = Readonly<{
  recordingId: string;
  guildId: string;
  channelId: string;
  producer: CraigProducerIdentity;
  actorObservationState: 'consistent' | 'conflicted';
  actors: CraigActor[];
  sealedReady: CraigLifecycleV3Event | null;
}>;

/**
 * Applies the Meeting consumer's fail-closed knowledge eligibility rule to a
 * producer-sealed roster. Automation, incomplete identity evidence, and any
 * contradictory observation are never eligible.
 */
export function selectCraigKnowledgeEligibleActorIds(
  ready: CraigLifecycleV3Event,
  authoritativeTrackActorIds: readonly string[]
): string[] {
  const parsed = parseV3Event(ready);
  if (parsed.type !== 'recording.authoritative_ready') throw new Error('Knowledge eligibility requires a sealed authoritative-ready event');
  if (!Array.isArray(authoritativeTrackActorIds)) throw new Error('Authoritative track actor identities are invalid');
  const trackActorIds = authoritativeTrackActorIds.map((actorId) => {
    if (typeof actorId !== 'string' || !discordSnowflake.test(actorId))
      throw new Error('Authoritative track actor identities are invalid');
    return actorId;
  });
  if (new Set(trackActorIds).size !== trackActorIds.length) throw new Error('Authoritative track actor identities are repeated');
  if (parsed.actorObservationState !== 'consistent') return [];
  const tracked = new Set(trackActorIds);
  return parsed.actors.filter(({ actorId, kind }) => kind === 'human' && tracked.has(actorId)).map(({ actorId }) => actorId);
}

export function deriveCraigActorFromDiscord(value: AuthenticatedDiscordActor): CraigActor {
  if (!isRecord(value) || !hasOnlyKeys(value, ['id', 'bot', 'system', 'webhook']) || typeof value.id !== 'string' || !discordSnowflake.test(value.id))
    throw new Error('Authenticated Discord actor is invalid');
  for (const key of ['bot', 'system', 'webhook'] as const)
    if (value[key] !== undefined && typeof value[key] !== 'boolean') throw new Error('Authenticated Discord actor signals are invalid');
  const kind: CraigActorKind =
    value.bot || value.system || value.webhook
      ? 'automation'
      : value.bot === false && value.system === false && value.webhook === false
      ? 'human'
      : 'unknown';
  return Object.freeze({ actorId: value.id, kind });
}

/** Producer-owned evidence. Callers can provide Discord signals, but cannot assert an actor kind. */
export class CraigActorObservationLedger {
  private readonly kindsByActor = new Map<string, CraigActorKind>();
  private conflicted = false;
  readonly producer: CraigProducerIdentity;

  constructor(producer: CraigProducerIdentity) {
    this.producer = freezeProducerIdentity(producer);
  }

  observeBatch(authenticatedActors: readonly AuthenticatedDiscordActor[]): () => void {
    if (!Array.isArray(authenticatedActors)) throw new Error('Craig actor batch is invalid');
    const actors = authenticatedActors.map(deriveCraigActorFromDiscord);
    const additions = new Set(actors.filter(({ actorId }) => !this.kindsByActor.has(actorId)).map(({ actorId }) => actorId));
    if (this.kindsByActor.size + additions.size > maximumCraigActorRosterSize) throw new Error('Craig actor roster exceeds its bounded size');

    const previousConflict = this.conflicted;
    const previous = new Map<string, CraigActorKind | undefined>();
    for (const actor of actors) if (!previous.has(actor.actorId)) previous.set(actor.actorId, this.kindsByActor.get(actor.actorId));
    // Mutation begins only after every input and the resulting size have been validated.
    for (const actor of actors) this.observeTrusted(actor);
    return () => {
      this.conflicted = previousConflict;
      for (const [actorId, kind] of previous) {
        if (kind === undefined) this.kindsByActor.delete(actorId);
        else this.kindsByActor.set(actorId, kind);
      }
    };
  }

  observationState(): 'consistent' | 'conflicted' {
    return this.conflicted ? 'conflicted' : 'consistent';
  }

  actors(): CraigActor[] {
    return [...this.kindsByActor].sort(([left], [right]) => left.localeCompare(right)).map(([actorId, kind]) => Object.freeze({ actorId, kind }));
  }

  private observeTrusted(actor: CraigActor): void {
    const existing = this.kindsByActor.get(actor.actorId);
    if (actor.kind === 'unknown') {
      if (existing === undefined) this.kindsByActor.set(actor.actorId, actor.kind);
      return;
    }
    if (existing === 'unknown' || existing === undefined) this.kindsByActor.set(actor.actorId, actor.kind);
    else if (existing !== actor.kind) {
      this.conflicted = true;
    }
  }

  static restoreActors(
    producer: CraigProducerIdentity,
    actors: readonly CraigActor[],
    state: 'consistent' | 'conflicted'
  ): CraigActorObservationLedger {
    const ledger = new CraigActorObservationLedger(producer);
    if (!Array.isArray(actors) || actors.length > maximumCraigActorRosterSize) throw new Error('Durable lifecycle snapshot actors are invalid');
    const parsed = actors.map(parseStoredActor);
    if (new Set(parsed.map(({ actorId }) => actorId)).size !== parsed.length) throw new Error('Durable lifecycle snapshot repeats an actor');
    for (const actor of parsed) ledger.observeTrusted(actor);
    if (state === 'conflicted') ledger.conflicted = true;
    else if (state !== 'consistent' || ledger.conflicted) throw new Error('Durable lifecycle snapshot state is invalid');
    return ledger;
  }
}

export class CraigLifecycleV3Producer {
  private readonly emitted: Array<Readonly<{ eventId: string; occurredAt: string; type: CraigLifecycleV3Event['type'] }>> = [];
  private readonly emittedIds = new Set<string>();
  private readonly pendingOutbox: CraigLifecycleV3Event[] = [];
  private readonly admissionRollbacks: Array<() => void> = [];
  private sealedReady: Extract<CraigLifecycleV3Event, { type: 'recording.authoritative_ready' }> | null = null;
  private readonly context: CraigLifecycleContext;
  private readonly ledger: CraigActorObservationLedger;

  constructor(context: CraigLifecycleContext, ledger: CraigActorObservationLedger) {
    this.context = freezeContext(context);
    this.ledger = ledger;
  }

  started(
    envelope: CraigLifecycleEnvelope,
    actors: readonly AuthenticatedDiscordActor[]
  ): Extract<CraigLifecycleV3Event, { type: 'meeting.started' }> {
    this.assertMutable();
    this.assertCanEmit(envelope, 2);
    const rollback = this.ledger.observeBatch(actors);
    return this.emit({ ...this.envelope(envelope), type: 'meeting.started', actors: this.ledger.actors(), rosterState: 'unsealed' }, rollback);
  }

  /** Records authenticated adapter evidence that does not itself imply a lifecycle transition. */
  observeActors(actors: readonly AuthenticatedDiscordActor[]): void {
    this.assertMutable();
    this.ledger.observeBatch(actors);
  }

  participant(
    envelope: CraigLifecycleEnvelope,
    type: 'participant.joined' | 'participant.left',
    actor: AuthenticatedDiscordActor
  ): Extract<CraigLifecycleV3Event, { type: 'participant.joined' | 'participant.left' }> {
    this.assertMutable();
    this.assertCanEmit(envelope, 2);
    const rollback = this.ledger.observeBatch([actor]);
    return this.emit({ ...this.envelope(envelope), type, actor: deriveCraigActorFromDiscord(actor) }, rollback);
  }

  connection(
    envelope: CraigLifecycleEnvelope,
    type: 'meeting.connection_lost' | 'meeting.connection_recovered',
    reason: string | null
  ): Extract<CraigLifecycleV3Event, { type: 'meeting.connection_lost' | 'meeting.connection_recovered' }> {
    this.assertMutable();
    assertReason(reason);
    this.assertCanEmit(envelope, 2);
    return this.emit({ ...this.envelope(envelope), type, reason });
  }

  terminal(
    envelope: CraigLifecycleEnvelope,
    type: 'meeting.ended' | 'meeting.aborted',
    reason: string | null
  ): Extract<CraigLifecycleV3Event, { type: 'meeting.ended' | 'meeting.aborted' }> {
    this.assertMutable();
    assertReason(reason);
    this.assertCanEmit(envelope, 1);
    return this.emit({ ...this.envelope(envelope), type, reason });
  }

  authoritativeReady(
    envelope: CraigLifecycleEnvelope,
    input: Readonly<{
      actors: readonly AuthenticatedDiscordActor[];
      endedAt: string;
      sourceFilesChecksumSha256: string;
      trackCount: number;
    }>
  ): Extract<CraigLifecycleV3Event, { type: 'recording.authoritative_ready' }> {
    assertEnvelope(envelope);
    assertContext(envelope, this.context);
    assertAuthoritativeReady(input);
    if (this.sealedReady !== null) {
      const retryLedger = CraigActorObservationLedger.restoreActors(this.ledger.producer, this.ledger.actors(), this.ledger.observationState());
      retryLedger.observeBatch(input.actors);
      const candidate = this.buildReadyCandidate(envelope, input, retryLedger.actors(), retryLedger.observationState());
      if (canonicalJson(candidate) !== canonicalJson(this.sealedReady)) throw new Error('Conflicting authoritative-ready retry after ledger seal');
      return deepFreeze(cloneEvent(this.sealedReady));
    }

    this.assertCanEmit(envelope);
    const rollback = this.ledger.observeBatch(input.actors);
    const ready = this.buildReadyCandidate(envelope, input, this.ledger.actors(), this.ledger.observationState());
    this.sealedReady = this.emit(ready, rollback);
    return deepFreeze(cloneEvent(this.sealedReady));
  }

  durableSnapshot(): DurableCraigLifecycleV3Snapshot {
    return deepFreeze({
      schemaVersion: 3 as const,
      ...this.context,
      producer: { ...this.ledger.producer },
      actorObservationState: this.ledger.observationState(),
      actors: this.ledger.actors(),
      emitted: this.emitted.map((item) => ({ ...item })),
      pendingOutbox: this.pendingOutbox.map(cloneEvent),
      sealedReady: this.sealedReady === null ? null : cloneEvent(this.sealedReady)
    });
  }

  durableAdmission(): CraigLifecycleV3Admission {
    return deepFreeze({
      ...this.context,
      producer: { ...this.ledger.producer },
      actorObservationState: this.ledger.observationState(),
      actors: this.ledger.actors(),
      sealedReady: this.sealedReady === null ? null : cloneEvent(this.sealedReady)
    });
  }

  /** Reverts only the most recently created, not-yet-admitted transition. */
  rollbackAdmission(event: CraigLifecycleV3Event): void {
    const last = this.pendingOutbox[this.pendingOutbox.length - 1];
    if (last?.eventId !== event.eventId) throw new Error('Lifecycle rollback is not the latest admission');
    this.pendingOutbox.pop();
    const removed = this.emitted.pop();
    if (removed !== undefined) this.emittedIds.delete(removed.eventId);
    this.admissionRollbacks.pop()?.();
    if (this.sealedReady?.eventId === event.eventId) this.sealedReady = null;
  }

  /**
   * Evicts payloads only after the integration sink has durably acknowledged
   * their delivery. Start, terminal, and ready evidence stay pinned because the
   * authoritative original-publication contract binds those exact events.
   */
  acknowledgeDelivered(eventId: string): void {
    const index = this.pendingOutbox.findIndex((event) => event.eventId === eventId);
    if (index < 0) return;
    const event = this.pendingOutbox[index];
    if (event.type === 'meeting.started' || event.type === 'meeting.ended' || event.type === 'meeting.aborted' || event.type === 'recording.authoritative_ready')
      return;
    this.pendingOutbox.splice(index, 1);
    this.admissionRollbacks.splice(index, 1);
  }

  private assertMutable(): void {
    if (this.sealedReady !== null) throw new Error('Craig lifecycle ledger is sealed');
  }

  private assertCanEmit(envelope: CraigLifecycleEnvelope, reservedFinalSlots = 0): void {
    if (this.pendingOutbox.length >= maximumCraigPendingLifecycleEvents - reservedFinalSlots)
      throw new Error('Craig lifecycle durable outbox capacity is exhausted');
    assertEnvelope(envelope);
    assertContext(envelope, this.context);
    if (this.emittedIds.has(envelope.eventId)) throw new Error('Craig lifecycle eventId was already emitted');
    const last = this.emitted[this.emitted.length - 1];
    if (last !== undefined && Date.parse(envelope.occurredAt) < Date.parse(last.occurredAt))
      throw new Error('Craig lifecycle timestamps must be ordered');
  }

  private envelope(value: CraigLifecycleEnvelope): CraigLifecycleV3Envelope {
    assertEnvelope(value);
    assertContext(value, this.context);
    return {
      ...value,
      schemaVersion: meetingLifecycleV3SchemaVersion,
      ...this.ledger.producer,
      actorObservationState: this.ledger.observationState()
    };
  }

  private emit<T extends CraigLifecycleV3Event>(event: T, rollback: () => void = () => undefined): T {
    if (this.pendingOutbox.length >= maximumCraigPendingLifecycleEvents)
      throw new Error('Craig lifecycle durable outbox capacity is exhausted');
    if (this.emittedIds.has(event.eventId)) throw new Error('Craig lifecycle eventId was already emitted');
    const last = this.emitted[this.emitted.length - 1];
    if (last !== undefined && Date.parse(event.occurredAt) < Date.parse(last.occurredAt))
      throw new Error('Craig lifecycle timestamps must be ordered');
    const frozen = deepFreeze(cloneEvent(event)) as T;
    this.emitted.push(Object.freeze({ eventId: event.eventId, occurredAt: event.occurredAt, type: event.type }));
    this.emittedIds.add(event.eventId);
    this.pendingOutbox.push(frozen);
    this.admissionRollbacks.push(rollback);
    return deepFreeze(cloneEvent(frozen)) as T;
  }

  private buildReadyCandidate(
    envelope: CraigLifecycleEnvelope,
    input: Readonly<{ endedAt: string; sourceFilesChecksumSha256: string; trackCount: number }>,
    actors: readonly CraigActor[],
    state: 'consistent' | 'conflicted'
  ): Extract<CraigLifecycleV3Event, { type: 'recording.authoritative_ready' }> {
    return {
      ...envelope,
      schemaVersion: meetingLifecycleV3SchemaVersion,
      ...this.ledger.producer,
      actorObservationState: state,
      type: 'recording.authoritative_ready',
      actors: actors.map((actor) => ({ ...actor })),
      rosterState: 'sealed',
      endedAt: input.endedAt,
      sourceFilesChecksumSha256: input.sourceFilesChecksumSha256,
      trackCount: input.trackCount
    };
  }

  static restore(
    config: Extract<MeetingLifecycleProducerConfiguration, { schemaVersion: 3 }>,
    expectedContext: CraigLifecycleContext,
    value: unknown
  ): CraigLifecycleV3Producer {
    const producer = freezeProducerIdentity(config);
    const context = freezeContext(expectedContext);
    if (
      !isRecord(value) ||
      !hasExactlyKeys(value, [
        'schemaVersion',
        'recordingId',
        'guildId',
        'channelId',
        'producer',
        'actorObservationState',
        'actors',
        'emitted',
        'pendingOutbox',
        'sealedReady'
      ]) ||
      value.schemaVersion !== 3 ||
      !isRecord(value.producer) ||
      !Array.isArray(value.actors) ||
      !Array.isArray(value.emitted) ||
      !Array.isArray(value.pendingOutbox)
    )
      throw new Error('Durable lifecycle snapshot is malformed');
    const restoredContext = freezeContext({
      recordingId: value.recordingId as string,
      guildId: value.guildId as string,
      channelId: value.channelId as string
    });
    if (!sameContext(restoredContext, context)) throw new Error('Durable lifecycle snapshot belongs to another recording context');
    const restoredProducer = parseProducerIdentity(value.producer);
    if (!sameProducer(restoredProducer, producer)) throw new Error('Durable lifecycle snapshot belongs to another producer revision');
    if (value.actorObservationState !== 'consistent' && value.actorObservationState !== 'conflicted')
      throw new Error('Durable lifecycle snapshot state is invalid');
    const ledger = CraigActorObservationLedger.restoreActors(producer, value.actors as CraigActor[], value.actorObservationState);
    const lifecycle = new CraigLifecycleV3Producer(context, ledger);

    const events = (value.pendingOutbox as unknown[]).map(parseV3Event);
    const emitted = (value.emitted as unknown[]).map(parseEmittedReference);
    if (new Set(emitted.map(({ eventId }) => eventId)).size !== emitted.length) throw new Error('Durable lifecycle snapshot repeats an event');
    for (let index = 1; index < emitted.length; index++)
      if (Date.parse(emitted[index].occurredAt) < Date.parse(emitted[index - 1].occurredAt))
        throw new Error('Durable lifecycle snapshot events are unordered');
    if (events.some((event) => !sameContext(event, context))) throw new Error('Durable lifecycle outbox event belongs to another recording context');
    if (events.some((event) => !sameProducer(event, producer))) throw new Error('Durable lifecycle outbox event belongs to another producer');
    const emittedById = new Map(emitted.map((item) => [item.eventId, item]));
    if (events.some((event) => emittedById.get(event.eventId)?.occurredAt !== event.occurredAt || emittedById.get(event.eventId)?.type !== event.type))
      throw new Error('Durable lifecycle outbox is not bound to emitted event order');
    let previousEmittedIndex = -1;
    for (const event of events) {
      const emittedIndex = emitted.findIndex(({ eventId }) => eventId === event.eventId);
      if (emittedIndex <= previousEmittedIndex) throw new Error('Durable lifecycle pending outbox is unordered');
      previousEmittedIndex = emittedIndex;
    }
    const pendingById = new Map(events.map((event) => [event.eventId, event]));
    for (const reference of emitted)
      if (isPinnedLifecycleType(reference.type)) {
        const pinned = pendingById.get(reference.eventId);
        if (pinned?.type !== reference.type || pinned.occurredAt !== reference.occurredAt)
          throw new Error('Durable lifecycle snapshot omits pinned evidence');
      }
    const readyEvents = events.filter(({ type }) => type === 'recording.authoritative_ready');
    if (readyEvents.length > 1 || (readyEvents.length === 1 && events[events.length - 1]?.type !== 'recording.authoritative_ready'))
      throw new Error('Durable lifecycle snapshot has an invalid seal order');
    lifecycle.emitted.push(...emitted.map((item) => Object.freeze({ ...item })));
    for (const { eventId } of emitted) lifecycle.emittedIds.add(eventId);
    lifecycle.pendingOutbox.push(...events.map((event) => deepFreeze(event)));

    if (value.sealedReady !== null) {
      const ready = parseV3Event(value.sealedReady);
      if (
        ready.type !== 'recording.authoritative_ready' ||
        !sameContext(ready, context) ||
        emittedById.get(ready.eventId)?.occurredAt !== ready.occurredAt ||
        emittedById.get(ready.eventId)?.type !== ready.type
      )
        throw new Error('Durable lifecycle snapshot seal is invalid');
      if (
        !sameProducer(ready, producer) ||
        ready.actorObservationState !== ledger.observationState() ||
        canonicalJson(ready.actors) !== canonicalJson(ledger.actors())
      )
        throw new Error('Durable lifecycle snapshot seal does not bind its final actor evidence');
      lifecycle.sealedReady = deepFreeze(ready);
    } else if (readyEvents.length !== 0) throw new Error('Durable lifecycle snapshot hides its seal');
    if (value.sealedReady !== null && canonicalJson(value.sealedReady) !== canonicalJson(readyEvents[0]))
      throw new Error('Durable lifecycle snapshot seal is not replayable');
    return lifecycle;
  }
}

export function parseMeetingLifecycleProducerConfiguration(value: unknown): MeetingLifecycleProducerConfiguration {
  if (!isRecord(value) || typeof value.schemaVersion !== 'number') throw new Error('Meeting lifecycle producer configuration is malformed');
  if (value.schemaVersion === 1) {
    if (!hasExactlyKeys(value, ['schemaVersion'])) throw new Error('Legacy producer configuration cannot claim v3 capabilities');
    return Object.freeze({ schemaVersion: 1 });
  }
  if (value.schemaVersion !== 3 || !hasExactlyKeys(value, ['schemaVersion', 'actorSemanticsVersion', 'producerCapabilityId', 'producerRevision']))
    throw new Error('Meeting lifecycle producer configuration has an unsupported version');
  return Object.freeze({
    schemaVersion: 3,
    ...parseProducerIdentity({
      actorSemanticsVersion: value.actorSemanticsVersion,
      producerCapabilityId: value.producerCapabilityId,
      producerRevision: value.producerRevision
    })
  });
}

export function createCraigLifecycleV3Producer(
  config: Extract<MeetingLifecycleProducerConfiguration, { schemaVersion: 3 }>,
  context: CraigLifecycleContext,
  snapshot?: unknown
): CraigLifecycleV3Producer {
  const parsedConfig = parseMeetingLifecycleProducerConfiguration(config);
  if (parsedConfig.schemaVersion !== 3) throw new Error('Lifecycle v3 requires an explicit v3 producer configuration');
  return snapshot === undefined
    ? new CraigLifecycleV3Producer(freezeContext(context), new CraigActorObservationLedger(parsedConfig))
    : CraigLifecycleV3Producer.restore(parsedConfig, context, snapshot);
}

export function restoreCraigLifecycleV3ProducerFromSnapshot(value: unknown): CraigLifecycleV3Producer {
  if (!isRecord(value) || !isRecord(value.producer)) throw new Error('Durable lifecycle snapshot is malformed');
  const producer = parseProducerIdentity(value.producer);
  const config = Object.freeze({ schemaVersion: 3 as const, ...producer });
  const context = freezeContext({
    recordingId: value.recordingId as string,
    guildId: value.guildId as string,
    channelId: value.channelId as string
  });
  return CraigLifecycleV3Producer.restore(config, context, value);
}

/** Contract tooling uses the same strict runtime parser as durable replay. */
export function validateCraigLifecycleV3Event(value: unknown): CraigLifecycleV3Event {
  return deepFreeze(parseV3Event(value));
}

function parseProducerIdentity(value: Record<string, unknown>): CraigProducerIdentity {
  if (!hasExactlyKeys(value, ['actorSemanticsVersion', 'producerCapabilityId', 'producerRevision']))
    throw new Error('Craig producer identity contains unknown fields');
  return freezeProducerIdentity(value as unknown as CraigProducerIdentity);
}

function freezeProducerIdentity(value: CraigProducerIdentity): CraigProducerIdentity {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, ['schemaVersion', 'actorSemanticsVersion', 'producerCapabilityId', 'producerRevision']) ||
    value.actorSemanticsVersion !== actorSemanticsVersion ||
    value.producerCapabilityId !== sealedActorRosterCapabilityId ||
    typeof value.producerRevision !== 'string' ||
    !immutableRevision.test(value.producerRevision)
  )
    throw new Error('Craig producer identity is invalid');
  return Object.freeze({
    actorSemanticsVersion,
    producerCapabilityId: sealedActorRosterCapabilityId,
    producerRevision: value.producerRevision
  });
}

function freezeContext(value: CraigLifecycleContext): CraigLifecycleContext {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, ['recordingId', 'guildId', 'channelId']) ||
    typeof value.recordingId !== 'string' ||
    value.recordingId.length < 1 ||
    value.recordingId.length > 128 ||
    typeof value.guildId !== 'string' ||
    !discordSnowflake.test(value.guildId) ||
    typeof value.channelId !== 'string' ||
    !discordSnowflake.test(value.channelId)
  )
    throw new Error('Craig lifecycle context is invalid');
  return Object.freeze({ recordingId: value.recordingId, guildId: value.guildId, channelId: value.channelId });
}

function parseStoredActor(value: unknown): CraigActor {
  if (!isRecord(value) || !hasExactlyKeys(value, ['actorId', 'kind']) || typeof value.actorId !== 'string' || !discordSnowflake.test(value.actorId))
    throw new Error('Durable lifecycle snapshot contains an invalid actor');
  if (value.kind !== 'human' && value.kind !== 'automation' && value.kind !== 'unknown')
    throw new Error('Durable lifecycle snapshot contains an invalid actor kind');
  return Object.freeze({ actorId: value.actorId, kind: value.kind });
}

function parseEmittedReference(value: unknown): Readonly<{ eventId: string; occurredAt: string; type: CraigLifecycleV3Event['type'] }> {
  if (
    !isRecord(value) ||
    !hasExactlyKeys(value, ['eventId', 'occurredAt', 'type']) ||
    typeof value.eventId !== 'string' ||
    typeof value.occurredAt !== 'string' ||
    !isLifecycleEventType(value.type)
  )
    throw new Error('Durable lifecycle snapshot emitted reference is invalid');
  assertEventIdentity(value.eventId, value.occurredAt);
  return Object.freeze({ eventId: value.eventId, occurredAt: value.occurredAt, type: value.type });
}

function isPinnedLifecycleType(type: CraigLifecycleV3Event['type']): boolean {
  return type === 'meeting.started' || type === 'meeting.ended' || type === 'meeting.aborted' || type === 'recording.authoritative_ready';
}

function isLifecycleEventType(value: unknown): value is CraigLifecycleV3Event['type'] {
  return (
    value === 'meeting.started' ||
    value === 'participant.joined' ||
    value === 'participant.left' ||
    value === 'meeting.connection_lost' ||
    value === 'meeting.connection_recovered' ||
    value === 'meeting.ended' ||
    value === 'meeting.aborted' ||
    value === 'recording.authoritative_ready'
  );
}

function parseV3Event(value: unknown): CraigLifecycleV3Event {
  if (!isRecord(value) || value.schemaVersion !== 3 || typeof value.type !== 'string') throw new Error('Durable lifecycle outbox event is invalid');
  const common = [
    'schemaVersion',
    'eventId',
    'recordingId',
    'guildId',
    'channelId',
    'occurredAt',
    'type',
    'actorObservationState',
    'actorSemanticsVersion',
    'producerCapabilityId',
    'producerRevision'
  ];
  const extraByType: Record<string, string[]> = {
    'meeting.started': ['actors', 'rosterState'],
    'participant.joined': ['actor'],
    'participant.left': ['actor'],
    'meeting.connection_lost': ['reason'],
    'meeting.connection_recovered': ['reason'],
    'meeting.ended': ['reason'],
    'meeting.aborted': ['reason'],
    'recording.authoritative_ready': ['actors', 'rosterState', 'endedAt', 'trackCount', 'sourceFilesChecksumSha256']
  };
  const extras = extraByType[value.type];
  if (extras === undefined || !hasExactlyKeys(value, [...common, ...extras]))
    throw new Error('Durable lifecycle outbox event contains unknown fields');
  assertEnvelope({
    eventId: value.eventId as string,
    recordingId: value.recordingId as string,
    guildId: value.guildId as string,
    channelId: value.channelId as string,
    occurredAt: value.occurredAt as string
  });
  parseProducerIdentity({
    actorSemanticsVersion: value.actorSemanticsVersion,
    producerCapabilityId: value.producerCapabilityId,
    producerRevision: value.producerRevision
  });
  if (value.actorObservationState !== 'consistent' && value.actorObservationState !== 'conflicted')
    throw new Error('Durable lifecycle outbox state is invalid');
  if (value.type === 'meeting.started' || value.type === 'recording.authoritative_ready') {
    if (!Array.isArray(value.actors) || value.actors.length > maximumCraigActorRosterSize)
      throw new Error('Durable lifecycle outbox actors are invalid');
    const actors = value.actors.map(parseStoredActor);
    if (new Set(actors.map(({ actorId }) => actorId)).size !== actors.length) throw new Error('Durable lifecycle outbox repeats an actor');
    if (value.type === 'meeting.started' && value.rosterState !== 'unsealed') throw new Error('Durable lifecycle started roster is invalid');
    if (value.type === 'recording.authoritative_ready') {
      if (value.rosterState !== 'sealed') throw new Error('Durable lifecycle ready roster is invalid');
      assertAuthoritativeReady(value as unknown as { endedAt: string; sourceFilesChecksumSha256: string; trackCount: number });
    }
  } else if (value.type === 'participant.joined' || value.type === 'participant.left') parseStoredActor(value.actor);
  else assertReason(value.reason as string | null);
  return JSON.parse(JSON.stringify(value)) as CraigLifecycleV3Event;
}

function assertEnvelope(value: CraigLifecycleEnvelope): void {
  if (!isRecord(value) || !hasExactlyKeys(value, ['eventId', 'recordingId', 'guildId', 'channelId', 'occurredAt']))
    throw new Error('Craig lifecycle envelope contains unknown fields');
  freezeContext({ recordingId: value.recordingId, guildId: value.guildId, channelId: value.channelId });
  if (typeof value.eventId !== 'string' || typeof value.occurredAt !== 'string') throw new Error('Craig lifecycle envelope is invalid');
  assertEventIdentity(value.eventId, value.occurredAt);
}

function assertEventIdentity(eventId: string, occurredAt: string): void {
  if (eventId.length < 1 || eventId.length > 128 || !isCanonicalLifecycleTimestamp(occurredAt))
    throw new Error('Craig lifecycle event identity is invalid');
}

function assertContext(value: CraigLifecycleContext, expected: CraigLifecycleContext): void {
  if (!sameContext(value, expected)) throw new Error('Craig lifecycle envelope belongs to another recording context');
}

function assertReason(value: string | null): void {
  if (value !== null && (typeof value !== 'string' || value.length < 1 || value.length > 256)) throw new Error('Craig lifecycle reason is invalid');
}

function assertAuthoritativeReady(value: Readonly<{ endedAt: string; sourceFilesChecksumSha256: string; trackCount: number }>): void {
  if (
    !isCanonicalLifecycleTimestamp(value.endedAt) ||
    !/^[0-9a-f]{64}$/.test(value.sourceFilesChecksumSha256) ||
    !Number.isSafeInteger(value.trackCount) ||
    value.trackCount < 1 ||
    value.trackCount > 64
  )
    throw new Error('Craig authoritative-ready evidence is invalid');
}

function isCanonicalLifecycleTimestamp(value: string): boolean {
  const parsedAt = Date.parse(value);
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) && Number.isFinite(parsedAt) && new Date(parsedAt).toISOString() === value;
}

function sameProducer(left: CraigProducerIdentity, right: CraigProducerIdentity): boolean {
  return (
    left.actorSemanticsVersion === right.actorSemanticsVersion &&
    left.producerCapabilityId === right.producerCapabilityId &&
    left.producerRevision === right.producerRevision
  );
}

function sameContext(left: CraigLifecycleContext, right: CraigLifecycleContext): boolean {
  return left.recordingId === right.recordingId && left.guildId === right.guildId && left.channelId === right.channelId;
}

function hasExactlyKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  return Object.keys(value).length === keys.length && keys.every((key) => Object.prototype.hasOwnProperty.call(value, key));
}

function hasOnlyKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  return Object.keys(value).every((key) => keys.includes(key));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function cloneEvent<T extends CraigLifecycleV3Event>(event: T): T {
  return JSON.parse(JSON.stringify(event)) as T;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(value);
}

function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null || Object.isFrozen(value)) return value;
  Object.freeze(value);
  for (const nested of Object.values(value as Record<string, unknown>)) deepFreeze(nested);
  return value;
}
