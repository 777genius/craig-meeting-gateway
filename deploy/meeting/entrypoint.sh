#!/bin/sh

set -eu
umask 077

DATABASE_URL_FILE=${DATABASE_URL_FILE:-/run/secrets/database_url}

require_secret_file() {
  secret_path=$1
  secret_name=$2

  if [ ! -f "$secret_path" ] || [ ! -r "$secret_path" ] || [ ! -s "$secret_path" ]; then
    echo "Craig startup: $secret_name must be a readable, non-empty mounted file at $secret_path" >&2
    exit 1
  fi
}

load_database_url() {
  require_secret_file "$DATABASE_URL_FILE" "database URL"
  DATABASE_URL=$(sed -e 's/[[:space:]]*$//' "$DATABASE_URL_FILE")
  case "$DATABASE_URL" in
    postgresql://*|postgres://*) ;;
    *)
      echo "Craig startup: database URL must use postgresql:// or postgres://" >&2
      exit 1
      ;;
  esac
  export DATABASE_URL
}

case "${1:-bot}" in
  migrate)
    load_database_url
    exec /app/node_modules/.bin/prisma migrate deploy --schema=/app/prisma/schema.prisma
    ;;
  bot)
    load_database_url
    require_secret_file "${DISCORD_BOT_TOKEN_FILE:-/run/secrets/discord_bot_token}" "Discord bot token"
    require_secret_file "${MEETING_INTEGRATION_TOKEN_FILE:-/run/secrets/meeting_integration_token}" "Meeting integration bearer"
    node /app/deploy/meeting/render-config.cjs
    : > /run/craig-config/startup.ready
    exec node --require=/app/deploy/meeting/node24-compat.cjs --enable-source-maps /app/apps/bot/dist/index.js
    ;;
  *)
    echo "Craig startup: unsupported command '$1'" >&2
    exit 64
    ;;
esac
