import { CRAIG_PLAYBACK_FRAME_SAMPLES } from './conversationPlayback';
import type { Chunk, RecordingUser } from './recording';

export const CRAIG_BOTIK_PLAYBACK_NAME = 'Botik';

export interface AuthoritativeBotikPlaybackTrack {
  user: RecordingUser;
  nextGranule?: number;
  nextRtpTimestamp?: number;
}

export interface AuthoritativeBotikPlaybackPacket {
  packetNo: number;
  chunk: Chunk;
}

/** Creates one local-only playback track; it is never part of the live packet tee. */
export function createAuthoritativeBotikPlaybackTrack(speakerId: string, track: number): AuthoritativeBotikPlaybackTrack {
  return {
    user: {
      id: speakerId,
      username: CRAIG_BOTIK_PLAYBACK_NAME,
      discriminator: '0',
      globalName: CRAIG_BOTIK_PLAYBACK_NAME,
      bot: true,
      unknown: false,
      track,
      packet: 2
    }
  };
}

/**
 * Reserves the exact Ogg sequence pair used by RecordingWriter.writeChunk.
 * Audio time follows the recording clock but cannot move backwards or overlap
 * the prior 20 ms Opus frame; RTP references advance exactly 960 samples.
 */
export function appendAuthoritativeBotikPlaybackPacket(
  track: AuthoritativeBotikPlaybackTrack,
  opusPacket: Buffer,
  recordingGranule: number
): AuthoritativeBotikPlaybackPacket {
  const observedGranule = Number.isSafeInteger(recordingGranule) && recordingGranule >= 0 ? recordingGranule : 0;
  const time = Math.max(observedGranule, track.nextGranule ?? observedGranule);
  const timestamp = track.nextRtpTimestamp ?? time >>> 0;
  const packetNo = track.user.packet;

  track.user.packet += 2;
  track.nextGranule = time + CRAIG_PLAYBACK_FRAME_SAMPLES;
  track.nextRtpTimestamp = (timestamp + CRAIG_PLAYBACK_FRAME_SAMPLES) >>> 0;

  return {
    packetNo,
    chunk: {
      data: Buffer.from(opusPacket),
      timestamp,
      time
    }
  };
}
