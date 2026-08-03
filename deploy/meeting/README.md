# Isolated Craig Meeting E2E deployment

This deployment runs only the Craig bot plus its PostgreSQL and Redis dependencies. It does not publish database or Redis ports and does not start or synchronize the bot during validation.

The image includes Craig's compiled Cook helpers and Debian's FFmpeg, FLAC, LAME, Opus and Vorbis tools. The optional `fdkaac` exporter is intentionally absent because it is not available from the pinned Debian base's official repositories and is not used by the Meeting E2E recording path.

Craig still pins `config@3.3.8`, which calls two `node:util` predicates removed by Node 24. A process-local compatibility preloader restores only those predicates before Craig starts; it does not patch installed dependencies or change upstream configuration defaults.

## Safety boundary

- Use an official Discord bot application, a private test guild, test-only channels and synthetic audio.
- Do not use a user account/self-bot token or production Meeting Platform credentials.
- The bot requests only Craig's existing unprivileged gateway intents: guilds, guild messages and guild voice states. Do not enable privileged intents in the Discord portal.
- Discord, database and Meeting integration credentials are read from mounted files. The generated Craig config exists only in `/run/craig-config` tmpfs.

## Prepare

1. Copy `.env.example` to `.env`, set the public Discord application ID, and set `CRAIG_SOURCE_REVISION` to the exact 40 or 64 character lowercase commit used as the build context. The same revision is used as the immutable image tag and OCI `org.opencontainers.image.revision` label.
2. Create the four files described in `secrets/README.md`, with mode `0600`. The PostgreSQL password in `database_url` must match `postgres_password`.
   Create the PostgreSQL data directory with owner UID/GID `70:70`, the Redis data directory with owner UID/GID `999:1000`, and the recording directory with owner UID/GID `10001:10001`.
3. Create the shared network if the parent Meeting Platform stack does not own it:

   ```sh
   docker network create discord-meeting-internal
   ```

4. Attach the Meeting Platform ingestion service to that network with DNS alias `meeting-platform`, or change `MEETING_INTEGRATION_URL` to its internal service name. Never point this at a host-published public endpoint for E2E.
5. Ensure the test bot's `/join`, `/stop` and recording commands were synchronized out of band and invite it only to the private test guild using the upstream bot/application-commands OAuth scopes.

The bot reads its authoritative auto-record channel snapshot from `GET /v1/craig/configuration` on the Meeting Platform service every `MEETING_AUTO_RECORD_CONFIGURATION_POLL_MS` (default: five seconds), using the existing mounted bearer token. `MEETING_AUTO_RECORD_CHANNEL_IDS` is an optional fail-closed static fallback until the first valid snapshot arrives; a successful empty snapshot disables auto-recording. `MEETING_AUTO_RECORD_SYNTHETIC_BOT_IDS` contains only synthetic audio bots that count toward automatic start/stop decisions. Both static lists are comma-separated Discord snowflake lists.

No browser login is needed after the official bot token has been mounted. The Discord web session is unrelated to the long-running bot gateway session.

## Validate without credentials or startup

`validate.sh` creates temporary placeholder files, runs `docker compose config --quiet`, removes the placeholders and starts nothing:

```sh
./deploy/meeting/validate.sh
```

For Dockerfile static checks and an image build:

```sh
docker build --check -f deploy/meeting/Dockerfile .
docker compose --env-file deploy/meeting/.env -f deploy/meeting/compose.yaml build
docker image inspect "${CRAIG_IMAGE_REPOSITORY:-craig-meeting-gateway}:${CRAIG_SOURCE_REVISION}" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}'
```

## Start and stop

Migrations complete before the bot starts. PostgreSQL and Redis are health-checked, and the bot is considered healthy only after its tmpfs config exists and both dependencies accept TCP connections.

```sh
docker compose --env-file deploy/meeting/.env -f deploy/meeting/compose.yaml up -d
docker compose --env-file deploy/meeting/.env -f deploy/meeting/compose.yaml ps
docker compose --env-file deploy/meeting/.env -f deploy/meeting/compose.yaml down
```

Craig receives `SIGINT` and has 30 seconds to disconnect cleanly. Recordings, PostgreSQL data and Redis data persist in separate directories below `CRAIG_DATA_ROOT`; `down` does not remove them.

## Parent-stack integration

The parent Meeting Platform deployment must attach its Craig ingestion service to `MEETING_NETWORK_NAME` and accept the same bearer mounted here. If the platform needs the authoritative original Craig files, mount `${CRAIG_DATA_ROOT}/recordings` read-only in its importer; live packet delivery is derived data and must not replace that recording.

The image bases are immutable multi-platform manifests: Node `24.18.1-bookworm-slim`, PostgreSQL `18.4-alpine`, Redis `8.8.1-alpine`, and Yarn `1.22.22`. Update a tag and digest together after checking the official registry manifest.
