# Session summary drain bound

A daemon summary pass scans at most **5,000 rows** for one session and has a
**250 ms** read and row-processing budget. Setup and commit can make its
measured end-to-end duration longer. The next daemon cycle resumes its durable
cursor. A row appended after a historical scan starts is outside its frozen
`scanBoundary`; it can require **one extra append-drain pass** even when its
observation time sorts before the historical cursor.

A scanned edit or erasure queues its rowid segment for repair. Each segment
contains at most **4,096 raw rowids**. One daemon cycle reads at most one
segment, so the raw-row scan budget for that session is bounded independently
of how many changed or future rows are due. Several due segments take several
cycles. A changed segment revision during a partial repair restarts only that
segment. No partial aggregate is sent; the daemon horizon advances only after
all queued repairs and append rows are reflected in an exact snapshot.

For a stable session with `N` eligible and skipped rows to scan, `R` segment
repair scans, `A` append-drain allowances, and `P` deliberate smaller proof
slices, the cycle proof uses this load-aware pass bound:

`ceil(N / 5,000) + floor(T / 250 ms) + R + A + P`

`T` is the sum of measured summary-update durations through completion. A
nonfinal pass must make row progress, reach its time cap, or expose pending
append work. The proof compares the sent wire to a full rebuild and verifies
that no partial session aggregate is sent. Its slow-read run adds latency to
each row as the summarizer consumes it and asserts that the 250 ms cap binds.
For the backdated-append fixture, the conservative bound uses `R = 1` repaired
segment, `A = 1` append or final empty-read allowance, and `P = 1` first pass
deliberately capped at 1,000 rows: `ceil(N / 5,000) + floor(T / 250 ms) + 3`.

This is a measured bound for an uninterrupted, progressing scan, not an
unconditional promise of `ceil(N / 5,000) + 1` cycles. A continuing stream of
edits can add segment repair scans, and future rows maturing in later horizons
can add more; neither resets an otherwise valid session prefix. A repeated
zero-progress read timeout has no finite drain bound until reads recover; it
must be reported as pending, never as caught up or sent.
