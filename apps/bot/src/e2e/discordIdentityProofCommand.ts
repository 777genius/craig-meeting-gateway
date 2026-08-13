import { createDiscordIdentityProof, DiscordIdentityProof, DiscordIdentityProofError } from './discordIdentityProof';

type ProofFactory = (environment: NodeJS.ProcessEnv) => Promise<DiscordIdentityProof>;

export async function runDiscordIdentityProofCommand(
  environment: NodeJS.ProcessEnv,
  write: (line: string) => void,
  createProof: ProofFactory = createDiscordIdentityProof
): Promise<number> {
  try {
    const proof = await createProof(environment);
    write(`${JSON.stringify(proof)}\n`);
    return 0;
  } catch (error) {
    const code = error instanceof DiscordIdentityProofError ? error.code : 'internal_error';
    write(`${JSON.stringify({ schemaVersion: 1, ok: false, code })}\n`);
    return 1;
  }
}
