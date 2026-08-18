import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

const root = path.resolve(__dirname, '../apps/bot/contracts/craig-lifecycle-v3');
const schemaFileName = 'craig-lifecycle-v3.schema.json';
const fixturesFileName = 'canonical-fixtures.json';
const checksumFileName = 'SHA256SUMS';
const bundleFileName = 'BUNDLE.sha256';

const digest = (bytes: Uint8Array): string => createHash('sha256').update(bytes).digest('hex');

async function generate(): Promise<void> {
  const [schema, fixtures] = await Promise.all([readFile(path.join(root, schemaFileName)), readFile(path.join(root, fixturesFileName))]);
  const parsed = JSON.parse(fixtures.toString('utf8')) as Record<string, unknown>;
  assert.equal(parsed.bundleVersion, 2, 'canonical fixture bundle version changed');
  assert.equal(parsed.contract, 'craig-lifecycle-v3', 'canonical fixture contract changed');
  assert.match(String(parsed.producerRevision), /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/);

  const checksums = `${digest(schema)}  ${schemaFileName}\n${digest(fixtures)}  ${fixturesFileName}\n`;
  const bundle = `${digest(Buffer.from(checksums, 'utf8'))}  ${checksumFileName}\n`;
  const write = process.argv.includes('--write');
  if (write) {
    await Promise.all([writeFile(path.join(root, checksumFileName), checksums, 'utf8'), writeFile(path.join(root, bundleFileName), bundle, 'utf8')]);
    return;
  }

  const [committedChecksums, committedBundle] = await Promise.all([
    readFile(path.join(root, checksumFileName), 'utf8'),
    readFile(path.join(root, bundleFileName), 'utf8')
  ]);
  assert.equal(committedChecksums, checksums, 'SHA256SUMS is stale; regenerate with --write');
  assert.equal(committedBundle, bundle, 'BUNDLE.sha256 is stale; regenerate with --write');
}

void generate().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
