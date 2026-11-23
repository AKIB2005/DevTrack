# Replay stream v2 writer

`SSE_V2_WRITER_ENABLED=true` enables the experimental v2 writer; it is disabled by default. Do not enable it during a rolling deployment with legacy writers. Drain or fence every legacy writer before enabling v2, and fence v2 writers before rolling back. The existing frontend prototype is not generation-aware and must not be used as a strict v2 sequence gate.

Each v2 stream entry stores `protocolVersion`, `generation` and `sequence`. A single-key Redis script reads the tail and appends the next sequence in the same generation. An empty stream or legacy tail starts a fresh UUID generation. The script refreshes the stream's 300-second TTL; trimming retains the newest entry and therefore its generation and sequence. Redis stream IDs remain the replay cursors. Legacy entries retain their existing format.

Stream expiration, data loss, manual deletion, and an intervening legacy write can start a new generation. A generation is not proof that all events were delivered. Redis failures, delayed publication after append, mixed writers, and retention loss still require separate recovery handling.
