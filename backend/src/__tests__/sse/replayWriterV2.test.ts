import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { describe, expect, it } from 'vitest';
import { appendReplayV2 } from '../../shared/sse/replayWriterV2.js';
import { READ_VERIFIED_REPLAY } from '../../shared/sse/sseHandler.js';

const url = process.env.SSE_TEST_REDIS_URL;
const suite = url ? describe : describe.skip;
const event = { type: 'runtime_state_patch', payload: { n: 1 }, timestamp: '2026-01-01T00:00:00Z', correlationId: 'c', schemaVersion: 1 };

suite('v2 replay writer with Redis', () => {
  it('reads a retained anchor and at most 101 successors atomically', async () => {
    const redis = new Redis(url!);
    const key = `test:sse:v2:${randomUUID()}`;
    try {
      expect(await redis.eval(READ_VERIFIED_REPLAY, 1, key, '1-0')).toEqual([0, []]);
      const anchor = await redis.xadd(key, '*', 'type', 'anchor');
      expect(await redis.eval(READ_VERIFIED_REPLAY, 1, key, anchor!)).toEqual([1, []]);
      for (let i = 0; i < 102; i++) await redis.xadd(key, '*', 'payload', i === 0 ? '{' : '{}');
      const result = await redis.eval(READ_VERIFIED_REPLAY, 1, key, anchor!) as [number, [string, string[]][]];
      expect(result[0]).toBe(1);
      expect(result[1]).toHaveLength(101);
      expect(result[1][0][1]).toEqual(['payload', '{']);
      await redis.xdel(key, anchor!);
      expect(await redis.eval(READ_VERIFIED_REPLAY, 1, key, anchor!)).toEqual([0, []]);
    } finally {
      try { await redis.del(key); } finally { await redis.quit(); }
    }
  });

  it('serializes concurrent appends and starts a new generation after legacy entries and recreation', async () => {
    const redis = new Redis(url!);
    const key = `test:sse:v2:${randomUUID()}`;
    try {
      await redis.xadd(key, '*', 'type', 'legacy', 'sequence', '100');
      const results = await Promise.all(Array.from({ length: 20 }, () => appendReplayV2(redis, key, event)));
      expect(new Set(results.map(item => item.generation)).size).toBe(1);
      expect(results.map(item => item.sequence).sort((a, b) => a - b)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
      const entries = await redis.xrange(key, '-', '+');
      expect(entries.slice(1).map(([id]) => results.find(item => item.id === id)?.sequence)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
      expect(entries.slice(1).map(([, fields]) => {
        const values = Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i], fields[2 * i + 1]]));
        return { sequence: Number(values.sequence), generation: values.generation };
      })).toEqual(Array.from({ length: 20 }, (_, i) => ({ sequence: i + 1, generation: results[0].generation })));
      await redis.xtrim(key, 'MAXLEN', '=', 1);
      const afterTrim = await appendReplayV2(redis, key, event);
      expect(afterTrim.sequence).toBe(21);
      expect(afterTrim.generation).toBe(results[0].generation);
      await redis.del(key);
      const recreated = await appendReplayV2(redis, key, event);
      expect(recreated.sequence).toBe(1);
      expect(recreated.generation).not.toBe(afterTrim.generation);
    } finally {
      try {
        await redis.del(key);
      } finally {
        await redis.quit();
      }
    }
  });
});
