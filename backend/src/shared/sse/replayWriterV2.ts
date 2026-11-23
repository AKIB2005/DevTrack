import { randomUUID } from 'node:crypto';
import type Redis from 'ioredis';

// One key keeps sequence and generation in the stream's own lifetime and cluster slot.
export const APPEND_REPLAY_V2 = `
local tail = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local generation = ARGV[1]
local sequence = 1
if #tail > 0 then
  local fields = tail[1][2]
  local version, previousGeneration, previousSequence
  for i = 1, #fields, 2 do
    if fields[i] == 'protocolVersion' then version = fields[i + 1] end
    if fields[i] == 'generation' then previousGeneration = fields[i + 1] end
    if fields[i] == 'sequence' then previousSequence = fields[i + 1] end
  end
  if version == '2' then
    local n = tonumber(previousSequence)
    if not previousGeneration or previousGeneration == '' or not n or n < 1 or n >= 9007199254740991 or n ~= math.floor(n) then
      return redis.error_reply('Invalid v2 stream tail')
    end
    generation = previousGeneration
    sequence = n + 1
  end
end
local sequenceString = string.format('%.0f', sequence)
local id = redis.call('XADD', KEYS[1], '*', 'type', ARGV[2], 'sequence', sequenceString,
  'payload', ARGV[3], 'timestamp', ARGV[4], 'correlationId', ARGV[5],
  'schemaVersion', ARGV[6], 'protocolVersion', '2', 'generation', generation)
redis.call('EXPIRE', KEYS[1], 300)
return {id, generation, sequenceString}
`;

export async function appendReplayV2(redis: Pick<Redis, 'eval'>, streamKey: string, event: {
  type: string; payload: Record<string, unknown>; timestamp: string; correlationId: string; schemaVersion: number;
}): Promise<{ id: string; generation: string; sequence: number }> {
  const result = await redis.eval(APPEND_REPLAY_V2, 1, streamKey, randomUUID(), event.type,
    JSON.stringify(event.payload), event.timestamp, event.correlationId, String(event.schemaVersion));
  if (!Array.isArray(result) || result.length !== 3 || typeof result[0] !== 'string' ||
      typeof result[1] !== 'string' || !Number.isSafeInteger(Number(result[2])) || Number(result[2]) < 1) {
    throw new Error('Invalid v2 replay append response');
  }
  return { id: result[0], generation: result[1], sequence: Number(result[2]) };
}
