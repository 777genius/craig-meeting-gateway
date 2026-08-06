'use strict';

const fs = require('node:fs');
const path = require('node:path');

const configDir = process.env.NODE_CONFIG_DIR || '/run/craig-config';
const discordTokenFile = process.env.DISCORD_BOT_TOKEN_FILE || '/run/secrets/discord_bot_token';
const meetingTokenFile = process.env.MEETING_INTEGRATION_TOKEN_FILE || '/run/secrets/meeting_integration_token';

function readSecret(secretPath, name) {
  const stat = fs.statSync(secretPath);
  if (!stat.isFile()) throw new Error(`${name} is not a regular mounted file`);
  const value = fs.readFileSync(secretPath, 'utf8').trim();
  if (!value) throw new Error(`${name} mounted file is empty`);
  return value;
}

function positiveInteger(name, fallback, maximum) {
  const raw = process.env[name] || String(fallback);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) throw new Error(`${name} must be an integer from 1 to ${maximum}`);
  return value;
}

function nonNegativeInteger(name, fallback, maximum) {
  const raw = process.env[name] || String(fallback);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0 || value > maximum) throw new Error(`${name} must be an integer from 0 to ${maximum}`);
  return value;
}

function strictBoolean(name, fallback) {
  const raw = process.env[name] || String(fallback);
  if (!['true', 'false'].includes(raw)) throw new Error(`${name} must be true or false`);
  return raw === 'true';
}

function snowflakeList(name, maximum) {
  const raw = process.env[name] || '';
  if (!raw.trim()) return [];
  const values = raw.split(',').map((value) => value.trim());
  if (values.length > maximum || new Set(values).size !== values.length || values.some((value) => !/^\d{17,20}$/.test(value)))
    throw new Error(`${name} must contain at most ${maximum} unique comma-separated Discord snowflakes`);
  return values;
}

const applicationID = process.env.DISCORD_APPLICATION_ID || '';
if (!/^\d{16,22}$/.test(applicationID)) throw new Error('DISCORD_APPLICATION_ID must be a Discord snowflake');

const endpoint = new URL(process.env.MEETING_INTEGRATION_URL || 'http://meeting-platform:4310');
if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password)
  throw new Error('MEETING_INTEGRATION_URL must be an HTTP(S) URL without embedded credentials');

const defaultPlaybackEndpoint = new URL('/v1/craig/playback', endpoint);
defaultPlaybackEndpoint.protocol = endpoint.protocol === 'https:' ? 'wss:' : 'ws:';
const playbackEndpoint = new URL(process.env.MEETING_PLAYBACK_URL || defaultPlaybackEndpoint.toString());
if (!['ws:', 'wss:'].includes(playbackEndpoint.protocol) || playbackEndpoint.username || playbackEndpoint.password)
  throw new Error('MEETING_PLAYBACK_URL must be a WS(S) URL without embedded credentials');

const discordToken = readSecret(discordTokenFile, 'Discord bot token');
readSecret(meetingTokenFile, 'Meeting integration bearer');
const meetingAutoRecordChannelIds = snowflakeList('MEETING_AUTO_RECORD_CHANNEL_IDS', 64);
const meetingAutoRecordSyntheticBotUserIds = snowflakeList('MEETING_AUTO_RECORD_SYNTHETIC_BOT_IDS', 128);

const generated = {
  redis: {
    host: process.env.REDIS_HOST || 'redis',
    port: positiveInteger('REDIS_PORT', 6379, 65535),
    keyPrefix: process.env.REDIS_KEY_PREFIX || 'craig-meeting:'
  },
  dexare: {
    token: discordToken,
    applicationID,
    craig: {
      recordingFolder: '/app/rec',
      meetingIntegration: {
        enabled: true,
        endpoint: endpoint.toString().replace(/\/$/, ''),
        tokenFile: meetingTokenFile,
        maxQueuedPackets: positiveInteger('MEETING_MAX_QUEUED_PACKETS', 8192, 262144),
        batchSize: positiveInteger('MEETING_PACKET_BATCH_SIZE', 128, 4096),
        requestTimeoutMs: positiveInteger('MEETING_REQUEST_TIMEOUT_MS', 5000, 60000)
      },
      meetingPlayback: {
        enabled: strictBoolean('MEETING_PLAYBACK_ENABLED', false),
        endpoint: playbackEndpoint.toString(),
        tokenFile: meetingTokenFile,
        connectionTimeoutMs: positiveInteger('MEETING_REQUEST_TIMEOUT_MS', 5000, 60000)
      },
      meetingAutoRecord: {
        enabled: true,
        channelIds: meetingAutoRecordChannelIds,
        syntheticBotUserIds: meetingAutoRecordSyntheticBotUserIds,
        startDelayMs: nonNegativeInteger('MEETING_AUTO_RECORD_START_DELAY_MS', 250, 60000),
        emptyGraceMs: nonNegativeInteger('MEETING_AUTO_RECORD_EMPTY_GRACE_MS', 5000, 60000),
        platformConfiguration: true,
        configurationPollMs: positiveInteger('MEETING_AUTO_RECORD_CONFIGURATION_POLL_MS', 5000, 60000)
      },
      webapp: {
        on: false
      }
    },
    logger: {
      level: process.env.CRAIG_LOG_LEVEL || 'info'
    }
  }
};

if (generated.dexare.craig.meetingIntegration.batchSize > generated.dexare.craig.meetingIntegration.maxQueuedPackets)
  throw new Error('MEETING_PACKET_BATCH_SIZE cannot exceed MEETING_MAX_QUEUED_PACKETS');

fs.mkdirSync(configDir, { recursive: true, mode: 0o700 });
const upstreamDefault = fs
  .readFileSync('/app/apps/bot/config/_default.js', 'utf8')
  .replace(/^const Eris = require\('eris'\);\r?\n/, '');
fs.writeFileSync(path.join(configDir, 'default.js'), upstreamDefault, { mode: 0o400 });
fs.writeFileSync(path.join(configDir, 'production.json'), `${JSON.stringify(generated, null, 2)}\n`, { mode: 0o400 });
