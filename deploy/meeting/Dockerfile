# syntax=docker/dockerfile:1.7

FROM node:24.18.1-bookworm-slim@sha256:235600a8101ab264e117b1768e925532262668dc9b581ef1dd7d96ced463b8e7 AS yarn-base

ENV COREPACK_HOME=/opt/corepack

RUN corepack enable \
  && corepack prepare yarn@1.22.22 --activate \
  && test "$(yarn --version)" = "1.22.22"

FROM yarn-base AS native-base

RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
    build-essential \
    ca-certificates \
    dbus-x11 \
    git \
    inkscape \
    pkg-config \
    python3 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY deploy/meeting/package.build.json ./package.json
COPY yarn.lock ./yarn.lock
COPY apps/bot/package.json apps/bot/package.json
COPY prisma/schema.prisma prisma/schema.prisma

FROM native-base AS build-dependencies

RUN yarn install --frozen-lockfile --non-interactive \
  && yarn cache clean

FROM native-base AS production-dependencies

RUN yarn install --frozen-lockfile --non-interactive --production=true \
  && ./node_modules/.bin/prisma generate --schema=/app/prisma/schema.prisma \
  && yarn cache clean

FROM build-dependencies AS build

COPY tsconfig.json ./tsconfig.json
COPY prisma ./prisma
COPY locale ./locale
COPY apps/bot ./apps/bot
COPY cook ./cook
COPY scripts/buildCook.sh ./scripts/buildCook.sh

RUN ./node_modules/.bin/prisma generate --schema=/app/prisma/schema.prisma \
  && yarn workspace craig-bot build \
  && /bin/sh ./scripts/buildCook.sh

FROM node:24.18.1-bookworm-slim@sha256:235600a8101ab264e117b1768e925532262668dc9b581ef1dd7d96ced463b8e7 AS runtime

ARG SOURCE_REVISION

RUN case "${SOURCE_REVISION}" in \
      ''|*[!0-9a-f]*) echo 'SOURCE_REVISION must be a lowercase 40 or 64 character hexadecimal commit' >&2; exit 1 ;; \
    esac \
  && { [ "${#SOURCE_REVISION}" -eq 40 ] || [ "${#SOURCE_REVISION}" -eq 64 ]; }

LABEL org.opencontainers.image.revision="${SOURCE_REVISION}"

RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
    at \
    ca-certificates \
    coreutils \
    ffmpeg \
    flac \
    lame \
    opus-tools \
    unzip \
    util-linux \
    vorbis-tools \
    zip \
  && rm -rf /var/lib/apt/lists/* \
  && groupadd --gid 10001 craig \
  && useradd --uid 10001 --gid craig --create-home --home-dir /home/craig --shell /usr/sbin/nologin craig \
  && install -d -o craig -g craig -m 0750 /app /app/rec

WORKDIR /app/apps/bot

ENV NODE_ENV=production \
    NODE_CONFIG_DIR=/run/craig-config

COPY --from=production-dependencies --chown=craig:craig /app/node_modules /app/node_modules
COPY --from=build --chown=craig:craig /app/apps/bot/dist /app/apps/bot/dist
COPY --from=build --chown=craig:craig /app/apps/bot/package.json /app/apps/bot/package.json
COPY --from=build --chown=craig:craig /app/apps/bot/config/_default.js /app/apps/bot/config/_default.js
COPY --from=build --chown=craig:craig /app/apps/bot/data /app/apps/bot/data
COPY --from=build --chown=craig:craig /app/apps/bot/emojis /app/apps/bot/emojis
COPY --from=build --chown=craig:craig /app/cook /app/cook
COPY --from=build --chown=craig:craig /app/locale /app/locale
COPY --from=build --chown=craig:craig /app/prisma /app/prisma
COPY --chown=craig:craig cook.sh /app/cook.sh
COPY --chown=root:root deploy/meeting/entrypoint.sh /app/deploy/meeting/entrypoint.sh
COPY --chown=root:root deploy/meeting/render-config.cjs /app/deploy/meeting/render-config.cjs
COPY --chown=root:root deploy/meeting/healthcheck.cjs /app/deploy/meeting/healthcheck.cjs
COPY --chown=root:root deploy/meeting/node24-compat.cjs /app/deploy/meeting/node24-compat.cjs

RUN chmod 0555 /app/deploy/meeting/entrypoint.sh \
  && chmod 0444 /app/deploy/meeting/render-config.cjs /app/deploy/meeting/healthcheck.cjs /app/deploy/meeting/node24-compat.cjs \
  && chmod 0555 /app/cook.sh /app/cook/*.sh /app/cook/*.js

USER 10001:10001

ENTRYPOINT ["/app/deploy/meeting/entrypoint.sh"]
CMD ["bot"]
