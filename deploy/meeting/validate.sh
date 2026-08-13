#!/bin/sh

set -eu

deploy_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
placeholder_dir=$(mktemp -d "${TMPDIR:-/tmp}/craig-meeting-validate.XXXXXX")
trap 'rm -rf "$placeholder_dir"' EXIT HUP INT TERM

validation_source_revision=0000000000000000000000000000000000000000

printf '%s\n' 'postgresql://craig_meeting:validation-only@db:5432/craig_meeting?schema=public' > "$placeholder_dir/database_url"
printf '%s\n' 'validation-only-discord-token' > "$placeholder_dir/discord_bot_token"
printf '%s\n' 'validation-only-integration-token' > "$placeholder_dir/meeting_integration_token"
printf '%s\n' 'validation-only-postgres-password' > "$placeholder_dir/postgres_password"

CRAIG_SECRETS_DIR=$placeholder_dir \
  CRAIG_SOURCE_REVISION=$validation_source_revision \
  docker compose \
    --env-file "$deploy_dir/.env.example" \
    --file "$deploy_dir/compose.yaml" \
    config --quiet

test_only_config=$(
  CRAIG_SECRETS_DIR=$placeholder_dir \
    CRAIG_SOURCE_REVISION=$validation_source_revision \
    docker compose \
      --env-file "$deploy_dir/.env.example" \
      --file "$deploy_dir/compose.yaml" \
      --file "$deploy_dir/compose.test-only.yaml" \
      config
)
printf '%s\n' "$test_only_config" | grep -q 'e2e.test-only: "true"'

echo "Craig Meeting base and test-only compose configurations and immutable source revision are valid. No containers were started."
