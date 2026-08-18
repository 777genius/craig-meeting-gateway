#!/bin/sh

set -eu

deploy_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
placeholder_dir=$(mktemp -d "${TMPDIR:-/tmp}/craig-meeting-validate.XXXXXX")
trap 'rm -rf "$placeholder_dir"' EXIT HUP INT TERM

validation_source_revision=0000000000000000000000000000000000000000
rendered_config="$placeholder_dir/compose.rendered.yaml"

printf '%s\n' 'postgresql://craig_meeting:validation-only@db:5432/craig_meeting?schema=public' > "$placeholder_dir/database_url"
printf '%s\n' 'validation-only-discord-token' > "$placeholder_dir/discord_bot_token"
printf '%s\n' 'validation-only-integration-token' > "$placeholder_dir/meeting_integration_token"
printf '%s\n' 'validation-only-postgres-password' > "$placeholder_dir/postgres_password"

CRAIG_SECRETS_DIR=$placeholder_dir \
  CRAIG_SOURCE_REVISION=$validation_source_revision \
  docker compose \
    --env-file "$deploy_dir/.env.example" \
    --file "$deploy_dir/compose.yaml" \
    config > "$rendered_config"

grep -Fq 'REDIS_HOST: craig-redis' "$rendered_config"
grep -Fq -- '- craig-redis' "$rendered_config"

echo "Craig Meeting compose configuration and immutable source revision are valid. No containers were started."
