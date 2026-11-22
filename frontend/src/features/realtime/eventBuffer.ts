export interface BufferedEvent<T> {
  id?: string;
  sequence?: number;
  value: T;
}

/** Orders events for one authenticated stream. */
export class EventSequenceBuffer<T> {
  private lastSequence: number | null = null;
  private readonly ids = new Set<string>();
  private readonly pending = new Map<number, BufferedEvent<T>>();
  private timer: ReturnType<typeof setTimeout> | null = null;
  private gapReported = false;
  private readonly deliver: (event: BufferedEvent<T>) => void;
  private readonly onGap: () => void;
  private readonly maxPending: number;

  constructor(
    deliver: (event: BufferedEvent<T>) => void,
    onGap: () => void,
    maxPending = 100,
  ) {
    this.deliver = deliver;
    this.onGap = onGap;
    this.maxPending = maxPending;
  }

  get hasGap(): boolean { return this.pending.size > 0 || this.gapReported; }

  acceptSnapshot(event: BufferedEvent<T>): void {
    if (event.id && this.ids.has(event.id)) return;
    this.clearTimer();
    this.pending.clear();
    this.lastSequence = null;
    this.gapReported = false;
    this.accept({ ...event, sequence: undefined });
  }

  accept(event: BufferedEvent<T>): void {
    if (event.id && (this.ids.has(event.id) || [...this.pending.values()].some(e => e.id === event.id))) return;
    const sequence = event.sequence;
    if (!Number.isSafeInteger(sequence) || !sequence || sequence < 0) {
      this.deliverEvent(event);
      return;
    }
    if (this.lastSequence === null) {
      this.lastSequence = sequence;
      this.deliverEvent(event);
      return;
    }
    if (sequence <= this.lastSequence || this.pending.has(sequence)) return;
    if (sequence === this.lastSequence + 1) {
      this.lastSequence = sequence;
      this.deliverEvent(event);
      while (this.pending.has(this.lastSequence + 1)) {
        const next = this.pending.get(this.lastSequence + 1)!;
        this.pending.delete(this.lastSequence + 1);
        this.lastSequence++;
        this.deliverEvent(next);
      }
      if (!this.pending.size) {
        this.clearTimer();
        this.gapReported = false;
      }
      return;
    }
    if (this.pending.size >= this.maxPending) {
      this.reportGap();
      return;
    }
    this.pending.set(sequence, event);
    this.resume();
  }

  private deliverEvent(event: BufferedEvent<T>): void {
    if (event.id) {
      this.ids.add(event.id);
      if (this.ids.size > 100) this.ids.delete(this.ids.values().next().value!);
    }
    this.deliver(event);
  }

  private reportGap(): void {
    this.clearTimer();
    if (this.gapReported) return;
    this.gapReported = true;
    this.onGap();
  }

  private clearTimer(): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
  }

  /** Pause the timer while the transport is disconnected. */
  reconnect(): void {
    this.clearTimer();
  }

  resume(): void {
    if (this.pending.size && !this.timer && !this.gapReported) {
      this.timer = setTimeout(() => { this.timer = null; this.reportGap(); }, 200);
    }
  }

  reset(): void {
    this.reconnect();
    this.pending.clear();
    this.lastSequence = null;
    this.ids.clear();
    this.gapReported = false;
  }

  dispose(): void { this.reset(); }
}
