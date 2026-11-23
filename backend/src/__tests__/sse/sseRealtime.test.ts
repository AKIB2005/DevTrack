import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const redis = {
  eval: vi.fn(),
  incr: vi.fn(async () => 1),
  expire: vi.fn(async () => 1),
  xtrim: vi.fn(async () => 0),
  xadd: vi.fn(async () => '1700000000000-0'),
  xrange: vi.fn(async (): Promise<unknown[]> => []),
};

vi.mock('../../shared/redis/client.js', () => ({
  getRedisClient: () => redis,
  getRedisHealth: () => ({ status: 'connected' }),
  isRedisConfigured: () => false,
  createRedisConnection: vi.fn(),
}));

vi.mock('../../modules/runtime-state/unifiedRuntimeState.service.js', () => ({
  unifiedRuntimeStateService: { getRuntimeState: vi.fn(async () => null) },
}));

import { EventBus, type SseClient } from '../../shared/sse/eventBus.js';
import { parseStreamId, replayMissedEvents } from '../../shared/sse/sseHandler.js';

function published(bus: EventBus) {
  const spy = vi.spyOn(bus, 'publishEnvelope').mockResolvedValue(undefined);
  return spy;
}

it('delivers replayable contest rating updates only to the owning user', async () => {
  const bus = new EventBus();
  const owner: string[] = []; const other: string[] = [];
  const decoder = new TextDecoder();
  for (const [userId, frames] of [['owner', owner], ['other', other]] as const) {
    bus.register({ id: `${userId}-client`, userId, connectedAt: Date.now(),
      controller: { enqueue: (value: Uint8Array) => frames.push(decoder.decode(value)) },
      emit: () => undefined, destroy: () => undefined } as unknown as SseClient);
  }
  try {
    const payload = { contestId: 'contest-123', platform: 'codeforces', rank: 12, ratingChange: -12 };
    await bus.publishEnvelope('owner', 'contest.rating_updated', payload);
    expect(owner).toHaveLength(1);
    expect(owner[0]).toContain('contest.rating_updated');
    expect(owner[0]).toContain('1700000000000-0');
    expect(other).toHaveLength(0);
    expect(redis.xadd).toHaveBeenCalledWith('sse:stream:owner', '*', 'type', 'contest.rating_updated',
      'sequence', '1', 'payload', JSON.stringify(payload), 'timestamp', expect.any(String),
      'correlationId', expect.any(String), 'schemaVersion', '1');
  } finally { bus.shutdown(); }
});

describe('runtime_state_patch coalescing', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('keeps one buffer per user, so concurrent users do not overwrite each other', async () => {
    const bus = new EventBus();
    const spy = published(bus);

    await bus.emitRuntimeStatePatch('user-a', { a: 1 });
    await bus.emitRuntimeStatePatch('user-b', { b: 2 });
    await bus.emitRuntimeStatePatch('user-a', { a2: 3 });
    await vi.advanceTimersByTimeAsync(3000);

    expect(spy).toHaveBeenCalledTimes(2);
    expect(spy).toHaveBeenCalledWith('user-a', 'runtime_state_patch', { a: 1, a2: 3 });
    expect(spy).toHaveBeenCalledWith('user-b', 'runtime_state_patch', { b: 2 });
    bus.shutdown();
  });

  it('starts a fresh window after a flush', async () => {
    const bus = new EventBus();
    const spy = published(bus);

    await bus.emitRuntimeStatePatch('user-a', { n: 1 });
    await vi.advanceTimersByTimeAsync(3000);
    await bus.emitRuntimeStatePatch('user-a', { n: 2 });
    await vi.advanceTimersByTimeAsync(3000);

    expect(spy.mock.calls.map((c) => c[2])).toEqual([{ n: 1 }, { n: 2 }]);
    bus.shutdown();
  });

  it('shutdown clears pending timers so nothing publishes afterwards', async () => {
    const bus = new EventBus();
    const spy = published(bus);

    await bus.emitRuntimeStatePatch('user-a', { a: 1 });
    await bus.emitRuntimeStatePatch('user-b', { b: 1 });
    bus.shutdown();
    await vi.advanceTimersByTimeAsync(10000);

    expect(spy).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('live event ids', () => {
  it('uses the opt-in v2 append result for live identity and metadata', async () => {
    process.env.SSE_V2_WRITER_ENABLED = 'true';
    const bus = new EventBus();
    const publish = vi.spyOn(bus, 'publish').mockImplementation(() => undefined);
    redis.eval.mockResolvedValueOnce(['1700000000123-2', 'generation', '1']);
    try {
      await bus.publishEnvelope('u', 'runtime_state_patch', { a: 1 });
      expect(redis.eval).toHaveBeenCalledWith(expect.any(String), 1, 'sse:stream:u', expect.any(String),
        'runtime_state_patch', '{"a":1}', expect.any(String), expect.any(String), '1');
      expect(publish).toHaveBeenCalledWith(expect.objectContaining({ id: '1700000000123-2',
        protocolVersion: 2, generation: 'generation', sequence: 1 }), 'u', '1700000000123-2');
    } finally {
      delete process.env.SSE_V2_WRITER_ENABLED;
      bus.shutdown();
    }
  });

  it('does not publish a v2 event when the append fails', async () => {
    process.env.SSE_V2_WRITER_ENABLED = 'true';
    const bus = new EventBus();
    const publish = vi.spyOn(bus, 'publish');
    redis.eval.mockRejectedValueOnce(new Error('append failed'));
    try {
      await expect(bus.publishEnvelope('u', 'runtime_state_patch', { a: 1 })).rejects.toThrow('append failed');
      expect(publish).not.toHaveBeenCalled();
    } finally {
      delete process.env.SSE_V2_WRITER_ENABLED;
      bus.shutdown();
    }
  });
  it('forwards the canonical ID to both shard publication paths', async () => {
    const bus = new EventBus();
    const shard = vi.spyOn(bus.streamAdapter, 'publishEvent').mockResolvedValue(null);
    const publish = vi.spyOn(bus, 'publish').mockImplementation(() => undefined);
    redis.xadd.mockResolvedValueOnce('1700000000123-2');
    await bus.publishEnvelope('u', 'runtime_state_patch', { a: 1 });
    expect(shard).toHaveBeenCalledWith(expect.objectContaining({ canonicalId: '1700000000123-2' }));
    expect(publish.mock.calls[0][0].id).toBe('1700000000123-2');
    expect(publish.mock.calls[0][2]).toBe('1700000000123-2');
    bus.shutdown();
  });

  it('delivers a canonical shard ID instead of its transport ID', () => {
    const bus = new EventBus();
    const frames: string[] = [];
    bus.register({ id: 'client', userId: 'u', connectedAt: Date.now(),
      controller: { enqueue: (b: Uint8Array) => frames.push(new TextDecoder().decode(b)) },
      emit: () => undefined, destroy: () => undefined } as unknown as SseClient);
    bus.streamAdapter.emit('message', { id: '999-0', canonicalId: '100-0', origin: 'peer',
      userId: 'u', type: 'runtime_state_patch', timestamp: 't', payload: {} });
    expect(frames[0]).toContain('id: 100-0\n');
    expect(frames[0]).not.toContain('999-0');
    bus.streamAdapter.emit('message', { id: '999-2', canonicalId: '100-0', origin: 'peer',
      userId: 'u', type: 'runtime_state_patch', timestamp: 't', payload: {} });
    expect(frames).toHaveLength(1);
    bus.streamAdapter.emit('message', { id: '999-1', origin: 'peer', userId: 'u',
      type: 'interview.turn.delta', timestamp: 't', payload: { delta: 'hi', seq: 1 } });
    expect(frames[1]).not.toMatch(/^id:/m);
    expect(frames[1]).not.toContain('999-1');
    bus.shutdown();
  });

  it('does not discard equal transport IDs belonging to different users', () => {
    const bus = new EventBus();
    const frames: string[] = [];
    for (const userId of ['u1', 'u2']) {
      bus.register({ id: userId, userId, connectedAt: Date.now(),
        controller: { enqueue: (b: Uint8Array) => frames.push(new TextDecoder().decode(b)) },
        emit: () => undefined, destroy: () => undefined } as unknown as SseClient);
      bus.streamAdapter.emit('message', { id: '999-0', origin: 'peer', userId,
        type: 'runtime_state_patch', timestamp: 't', payload: { userId } });
    }
    expect(frames).toHaveLength(2);
    expect(frames.join('')).toContain('u1');
    expect(frames.join('')).toContain('u2');
    bus.shutdown();
  });

  it('deduplicates legacy transport redelivery while preserving distinct ID-less events', () => {
    const bus = new EventBus();
    const frames: string[] = [];
    bus.register({ id: 'c', userId: 'u', connectedAt: Date.now(),
      controller: { enqueue: (b: Uint8Array) => frames.push(new TextDecoder().decode(b)) },
      emit: () => undefined, destroy: () => undefined } as unknown as SseClient);
    const legacy = { id: '999-0', origin: 'peer', userId: 'u', type: 'interview.turn.delta', timestamp: 't', payload: { seq: 1 } };
    bus.streamAdapter.emit('message', legacy);
    bus.streamAdapter.emit('message', legacy);
    bus.streamAdapter.emit('message', { ...legacy, id: '999-1', payload: { seq: 2 } });
    bus.streamAdapter.emit('message', { ...legacy, id: undefined, payload: { seq: 3 } });
    bus.streamAdapter.emit('message', { ...legacy, id: undefined, payload: { seq: 4 } });
    expect(frames).toHaveLength(4);
    expect(frames.every(frame => !/^id:/m.test(frame))).toBe(true);
    bus.shutdown();
  });

  it('publishes the Redis Stream id the entry was stored under', async () => {
    const bus = new EventBus();
    const seen: string[] = [];
    bus.on('event', (e: { id: string }) => seen.push(e.id));
    const publish = vi.spyOn(bus, 'publish').mockImplementation(() => undefined);

    redis.xadd.mockResolvedValueOnce('1700000000123-2');
    await bus.publishEnvelope('user-with-dashes', 'runtime_state_patch', { a: 1 });

    expect(publish.mock.calls[0][0].id).toBe('1700000000123-2');
    bus.shutdown();
  });
});

describe('parseStreamId', () => {
  it.each([
    ['1700000000000-0', '1700000000000-0'],
    ['1700000000000-1', '1700000000000-1'],
    ['1700000000000-2', '1700000000000-2'],
    [' 5-7 ', '5-7'],
  ])('accepts %s', (input, expected) => {
    expect(parseStreamId(input)).toBe(expected);
  });

  it.each(['', undefined, 'abc', 'user-1-2', 'user-with-dash-5-1700', '123', '123-', '-5', '1-2-3', '1.5-2'])(
    'rejects %s',
    (input) => {
      expect(parseStreamId(input as string | undefined)).toBeNull();
    },
  );
});

describe('replayMissedEvents', () => {
  const entry = (id: string, seq: number): [string, string[]] => [
    id,
    ['type', 'runtime_state_patch', 'sequence', String(seq), 'payload', '{"x":1}', 'timestamp', 't'],
  ];

  function controller() {
    const chunks: string[] = [];
    const ctl = { enqueue: (b: Uint8Array) => chunks.push(new TextDecoder().decode(b)) };
    return { ctl: ctl as unknown as ReadableStreamDefaultController<Uint8Array>, chunks };
  }

  beforeEach(() => redis.xrange.mockClear());

  describe('verified v2 replay', () => {
    beforeEach(() => { redis.eval.mockReset(); });

    it('replays a verified anchor suffix with v2 metadata', async () => {
      redis.eval.mockResolvedValueOnce([1, [['2-0', ['type', 'runtime_state_patch', 'sequence', '2',
        'timestamp', 't', 'payload', '{}', 'protocolVersion', '2', 'generation', 'g']]]]);
      const { ctl, chunks } = controller();
      expect([...await replayMissedEvents('u', '1-0', ctl, true)]).toEqual(['2-0']);
      expect(chunks[0]).toContain('"generation":"g"');
      expect(chunks[0]).toContain('"sequence":2');
    });

    it.each([
      ['invalid_cursor', 'bad', undefined],
      ['cursor_unavailable', '1-0', [0, []]],
      ['replay_limit_exceeded', '1-0', [1, Array.from({ length: 101 }, () => entry('2-0', 2))]],
      ['replay_failed', '1-0', [1, [['2-0', ['type', 'x', 'sequence', '2', 'timestamp', 't', 'payload', '{']]]]],
      ['replay_failed', '1-0', [1, [['2-0', ['type', 'x', 'sequence', '2', 'timestamp', 't', 'payload', '{}', 'protocolVersion', '2']]]]],
    ])('signals %s without advancing the cursor', async (reason, cursor, result) => {
      if (result) redis.eval.mockResolvedValueOnce(result);
      const { ctl, chunks } = controller();
      expect((await replayMissedEvents('u', cursor as string, ctl, true)).size).toBe(0);
      expect(chunks).toHaveLength(1);
      expect(chunks[0]).toContain('event: sse_recovery_required\n');
      expect(chunks[0]).toContain(`"reason":"${reason}"`);
      expect(chunks[0]).toContain('"replayComplete":false');
      expect(chunks[0]).not.toMatch(/^id:/m);
    });

    it('signals Redis failures and accepts an empty verified suffix', async () => {
      const first = controller();
      redis.eval.mockRejectedValueOnce(new Error('Redis unavailable'));
      await replayMissedEvents('u', '1-0', first.ctl, true);
      expect(first.chunks[0]).toContain('"reason":"replay_failed"');
      const second = controller();
      redis.eval.mockResolvedValueOnce([1, []]);
      await replayMissedEvents('u', '1-0', second.ctl, true);
      expect(second.chunks).toEqual([]);
    });

    it('signals malformed data before delivering an earlier valid entry', async () => {
      redis.eval.mockResolvedValueOnce([1, [entry('2-0', 2), ['3-0', ['payload', '{']]]]);
      const { ctl, chunks } = controller();
      expect((await replayMissedEvents('u', '1-0', ctl, true)).size).toBe(0);
      expect(chunks).toHaveLength(1);
      expect(chunks[0]).toContain('"reason":"replay_failed"');
      expect(chunks[0]).not.toMatch(/^id:/m);
    });
  });

  it('preserves v2 metadata in replay', async () => {
    redis.xrange.mockResolvedValueOnce([['1700000000001-0', [
      'type', 'runtime_state_patch', 'sequence', '1', 'payload', '{}', 'timestamp', 't',
      'protocolVersion', '2', 'generation', 'generation',
    ]]]);
    const { ctl, chunks } = controller();
    await replayMissedEvents('u', '1700000000000-0', ctl);
    expect(chunks[0]).toContain('"protocolVersion":2');
    expect(chunks[0]).toContain('"generation":"generation"');
    expect(chunks[0]).toContain('id: 1700000000001-0');
  });

  it('replays the IDs returned to concurrent live publishers', async () => {
    const bus = new EventBus();
    const publish = vi.spyOn(bus, 'publish').mockImplementation(() => undefined);
    redis.xadd.mockResolvedValueOnce('1700000000001-0').mockResolvedValueOnce('1700000000001-1');
    await Promise.all([
      bus.publishEnvelope('u', 'runtime_state_patch', { x: 1 }),
      bus.publishEnvelope('u', 'runtime_state_patch', { x: 2 }),
    ]);
    const liveIds = publish.mock.calls.map(([event]) => event.id);
    redis.xrange.mockResolvedValueOnce([entry('1700000000001-0', 1), entry('1700000000001-1', 2)]);
    const { ctl, chunks } = controller();
    await replayMissedEvents('u', '1700000000000-0', ctl);
    expect(chunks.map(chunk => chunk.split('\n')[0].slice(4))).toEqual(liveIds);
    bus.shutdown();
  });

  it.each(['1700000000000-0', '1700000000000-1', '1700000000000-2'])(
    'asks Redis for entries strictly after %s',
    async (lastId) => {
      redis.xrange.mockResolvedValueOnce([entry('1700000000001-0', 4)]);
      const { ctl, chunks } = controller();

      await replayMissedEvents('user-with-dashes', lastId, ctl);

      expect(redis.xrange).toHaveBeenCalledWith('sse:stream:user-with-dashes', `(${lastId}`, '+', 'COUNT', 50);
      expect(chunks).toHaveLength(1);
      expect(chunks[0]).toContain('id: 1700000000001-0');
    },
  );

  it('replays several events from the same millisecond in order', async () => {
    redis.xrange.mockResolvedValueOnce([entry('1700000000000-1', 2), entry('1700000000000-2', 3)]);
    const { ctl, chunks } = controller();

    await replayMissedEvents('u', '1700000000000-0', ctl);

    expect(chunks.map((c) => c.split('\n')[0])).toEqual(['id: 1700000000000-1', 'id: 1700000000000-2']);
  });

  it.each(['garbage', 'u-1-1700000000000', 'NaN-1'])('does not query Redis for malformed id %s', async (bad) => {
    const { ctl, chunks } = controller();

    await replayMissedEvents('u', bad, ctl);

    expect(redis.xrange).not.toHaveBeenCalled();
    expect(chunks).toHaveLength(0);
  });
});
