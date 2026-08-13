export type MeetingActorKind = 'human' | 'automation' | 'unknown';

export interface MeetingActor {
  actorId: string;
  kind: MeetingActorKind;
}

/**
 * The deliberately small Discord shape accepted at the provider boundary.
 * Domain code must not derive actor kinds from cached participant IDs.
 */
export interface DiscordActorIdentity {
  id?: unknown;
  bot?: unknown;
  applicationId?: unknown;
  user?: {
    id?: unknown;
    bot?: unknown;
  } | null;
}

const discordSnowflake = /^\d{17,20}$/;

/** Compares opaque provider identifiers by Unicode code unit, independent of host locale. */
export function compareOpaqueDiscordIds(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function classifyDiscordActor(
  actorId: string,
  identity: DiscordActorIdentity | null | undefined,
  recorderActorId: string
): MeetingActor {
  assertActorId(actorId);
  assertActorId(recorderActorId);
  if (actorId === recorderActorId) return Object.freeze({ actorId, kind: 'automation' });
  if (!identity) return Object.freeze({ actorId, kind: 'unknown' });

  const identityIds = [identity.id, identity.user?.id].filter((value): value is string => typeof value === 'string');
  if (identityIds.length === 0 || identityIds.some((id) => id !== actorId))
    return Object.freeze({ actorId, kind: 'unknown' });

  const botSignals = [identity.bot, identity.user?.bot].filter((value): value is boolean => typeof value === 'boolean');
  if (botSignals.some(Boolean) && botSignals.some((value) => !value)) return Object.freeze({ actorId, kind: 'unknown' });
  if (botSignals.includes(true) || typeof identity.applicationId === 'string')
    return Object.freeze({ actorId, kind: 'automation' });
  if (botSignals.includes(false)) return Object.freeze({ actorId, kind: 'human' });
  return Object.freeze({ actorId, kind: 'unknown' });
}

export class RecordingActorRegistry {
  private readonly actorsById = new Map<string, MeetingActor>();

  register(actor: MeetingActor): MeetingActor {
    assertActorId(actor.actorId);
    if (actor.kind !== 'human' && actor.kind !== 'automation' && actor.kind !== 'unknown') throw new Error('Actor kind is invalid');
    const existing = this.actorsById.get(actor.actorId);
    if (existing) {
      if (existing.kind !== actor.kind) throw new Error(`Actor ${actor.actorId} cannot change kind from ${existing.kind} to ${actor.kind}`);
      return existing;
    }
    const immutable = Object.freeze({ actorId: actor.actorId, kind: actor.kind });
    this.actorsById.set(actor.actorId, immutable);
    return immutable;
  }

  get(actorId: string): MeetingActor | undefined {
    return this.actorsById.get(actorId);
  }

  roster(): readonly MeetingActor[] {
    return Object.freeze(
      [...this.actorsById.values()]
        .sort((left, right) => compareOpaqueDiscordIds(left.actorId, right.actorId))
        .map((actor) => Object.freeze({ ...actor }))
    );
  }
}

export function validateActorRoster(value: unknown): readonly MeetingActor[] {
  if (!Array.isArray(value) || value.length > 1000) throw new Error('Actor roster is malformed');
  const registry = new RecordingActorRegistry();
  for (const candidate of value) {
    if (
      candidate === null ||
      typeof candidate !== 'object' ||
      Array.isArray(candidate) ||
      Object.keys(candidate).length !== 2 ||
      !('actorId' in candidate) ||
      !('kind' in candidate)
    )
      throw new Error('Actor roster entry is malformed');
    const actor = candidate as { actorId: unknown; kind: unknown };
    if (typeof actor.actorId !== 'string' || typeof actor.kind !== 'string') throw new Error('Actor roster entry is invalid');
    if (registry.get(actor.actorId)) throw new Error(`Actor roster repeats actor ${actor.actorId}`);
    registry.register(actor as MeetingActor);
  }
  return registry.roster();
}

function assertActorId(actorId: string): void {
  if (!discordSnowflake.test(actorId)) throw new Error('Actor ID is not a Discord snowflake');
}
