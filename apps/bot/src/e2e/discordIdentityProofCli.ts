import { runDiscordIdentityProofCommand } from './discordIdentityProofCommand';

runDiscordIdentityProofCommand(process.env, (line) => process.stdout.write(line)).then((exitCode) => {
  process.exitCode = exitCode;
});
