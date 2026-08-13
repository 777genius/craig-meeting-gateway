import { readDiscordBotSecretSnapshot } from './discordIdentityProof';
import { runDiscordIdentityProofCommand } from './discordIdentityProofCommand';

const path = process.argv[2] || '';
runDiscordIdentityProofCommand(
  process.env,
  (line) => process.stdout.write(line),
  async () => {
    await readDiscordBotSecretSnapshot(path);
    throw new Error('unexpected secret acceptance');
  }
).then((exitCode) => {
  process.exitCode = exitCode;
});
