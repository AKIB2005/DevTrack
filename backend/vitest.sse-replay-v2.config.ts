import { defineConfig } from 'vitest/config';

// This Redis-only suite does not need the default MongoDB setup.
export default defineConfig({
  test: { include: ['src/__tests__/sse/replayWriterV2.test.ts'] },
});
