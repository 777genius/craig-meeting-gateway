import { InteractionType } from 'slash-create';

export interface RegisteredSlashCommand {
  commandName: string;
  type: number;
  guildIDs?: readonly string[];
}

interface GatewayInteraction {
  type?: unknown;
  guild_id?: unknown;
  data?: {
    name?: unknown;
    type?: unknown;
  };
}

interface GatewayEvent {
  t?: unknown;
  d?: unknown;
}

function isGatewayInteraction(value: unknown): value is GatewayInteraction {
  return typeof value === 'object' && value !== null;
}

function isOwnedApplicationCommand(interaction: GatewayInteraction, commands: Iterable<RegisteredSlashCommand>) {
  const { data } = interaction;
  if (!data || typeof data.name !== 'string' || typeof data.type !== 'number') return false;

  const guildID = typeof interaction.guild_id === 'string' ? interaction.guild_id : undefined;
  for (const command of commands) {
    if (command.commandName !== data.name || command.type !== data.type) continue;
    if (!command.guildIDs || (guildID && command.guildIDs.includes(guildID))) return true;
  }

  return false;
}

export function shouldHandleCraigGatewayInteraction(interaction: unknown, commands: Iterable<RegisteredSlashCommand>) {
  if (!isGatewayInteraction(interaction)) return false;

  if (interaction.type !== InteractionType.APPLICATION_COMMAND && interaction.type !== InteractionType.APPLICATION_COMMAND_AUTOCOMPLETE) return true;

  return isOwnedApplicationCommand(interaction, commands);
}

export function forwardOwnedCraigGatewayInteraction(
  event: GatewayEvent,
  commands: Iterable<RegisteredSlashCommand>,
  handler: (interaction: any) => void
) {
  if (event.t !== 'INTERACTION_CREATE' || !shouldHandleCraigGatewayInteraction(event.d, commands)) return;
  handler(event.d);
}
