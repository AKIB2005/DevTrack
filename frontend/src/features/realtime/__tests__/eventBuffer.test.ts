import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { EventSequenceBuffer } from '../eventBuffer';

describe('EventSequenceBuffer', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  function setup(maxPending?: number) {
    const delivered: number[] = [];
    const onGap = vi.fn();
    const buffer = new EventSequenceBuffer<number>(event => delivered.push(event.value), onGap, maxPending);
    const send = (sequence: number | undefined, id = String(sequence), value = sequence ?? 0) =>
      buffer.accept({ id, sequence, value });
    return { buffer, delivered, onGap, send };
  }

  it('delivers the first and contiguous events synchronously and rejects duplicate IDs and sequences', () => {
    const { send, delivered } = setup();
    send(1, 'a'); send(2, 'b'); send(3, 'b'); send(2, 'c');
    expect(delivered).toEqual([1, 2]);
  });

  it('drains multiple out-of-order events as soon as the gap closes', () => {
    const { send, delivered, onGap } = setup();
    send(1); send(4); send(3); send(2);
    expect(delivered).toEqual([1, 2, 3, 4]);
    vi.advanceTimersByTime(200);
    expect(onGap).not.toHaveBeenCalled();
  });

  it('reports an unresolved gap after 200ms without delivering past it', () => {
    const { send, delivered, onGap, buffer } = setup();
    send(1); send(3);
    vi.advanceTimersByTime(199);
    expect(onGap).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onGap).toHaveBeenCalledTimes(1);
    expect(buffer.hasGap).toBe(true);
    expect(delivered).toEqual([1]);
    send(2);
    expect(delivered).toEqual([1, 2, 3]);
  });

  it('bounds recent IDs and pending events independently', () => {
    const { send, delivered, onGap } = setup(2);
    for (let i = 1; i <= 101; i++) send(undefined, String(i), i);
    send(undefined, '1', 102);
    send(undefined, '3', 103);
    expect(delivered).toHaveLength(102);
    send(1, 'first'); send(3, 'future-3'); send(4, 'future-4'); send(5, 'future-5');
    expect(onGap).toHaveBeenCalledTimes(1);
    send(2, 'next-2');
    expect(delivered.slice(-3)).toEqual([2, 3, 4]);
  });

  it('cleans timers on reconnect, reset and dispose, and reset permits a new counter', () => {
    const { send, delivered, onGap, buffer } = setup();
    send(10); send(12); buffer.reconnect();
    vi.advanceTimersByTime(200);
    expect(onGap).not.toHaveBeenCalled();
    send(11); send(13); buffer.reset();
    send(1); send(3); buffer.dispose();
    vi.advanceTimersByTime(200);
    expect(onGap).not.toHaveBeenCalled();
    expect(delivered).toEqual([10, 11, 12, 13, 1]);
  });

  it('preserves unsequenced delivery and keeps a reported gap across reconnect', () => {
    const { send, delivered, buffer, onGap } = setup();
    send(1); send(3); vi.advanceTimersByTime(200); buffer.reconnect();
    send(undefined, 'legacy', 9); send(3);
    vi.advanceTimersByTime(1000);
    expect(delivered).toEqual([1, 9]);
    expect(onGap).toHaveBeenCalledTimes(1);
    expect(buffer.hasGap).toBe(true);
  });

  it('retains a gap across transport reconnect and resumes the tolerance timer', () => {
    const { send, delivered, buffer, onGap } = setup();
    send(1); send(3);
    buffer.reconnect();
    expect(buffer.hasGap).toBe(true);
    vi.advanceTimersByTime(500);
    expect(onGap).not.toHaveBeenCalled();
    buffer.resume();
    vi.advanceTimersByTime(199);
    send(2);
    vi.advanceTimersByTime(1);
    expect(delivered).toEqual([1, 2, 3]);
    expect(onGap).not.toHaveBeenCalled();
  });

  it('uses a snapshot as a new baseline without redelivering the same snapshot', () => {
    const { send, delivered, buffer } = setup();
    send(10); send(12);
    buffer.acceptSnapshot({ id: 'snapshot', value: 99, sequence: 0 });
    buffer.acceptSnapshot({ id: 'snapshot', value: 99, sequence: 0 });
    send(1, 'new-epoch');
    expect(delivered).toEqual([10, 99, 1]);
    expect(buffer.hasGap).toBe(false);
  });
});
