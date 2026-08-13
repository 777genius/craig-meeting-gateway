import { createDiscordIdentityProof, DiscordIdentityProofError } from './discordIdentityProof';

createDiscordIdentityProof(process.env).then(
  (proof) => process.stdout.write(`${JSON.stringify(proof)}\n`),
  (error: unknown) => {
    const code = error instanceof DiscordIdentityProofError ? error.code : 'internal_error';
    process.stdout.write(`${JSON.stringify({ schemaVersion: 1, ok: false, code })}\n`);
    process.exitCode = 1;
  }
);
