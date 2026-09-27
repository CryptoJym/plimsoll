# Session summary drain bound

A daemon summary pass scans at most **5,000 rows** for one session and has a
**250 ms** read and row-processing budget. Setup and commit can make its
measured end-to-end duration longer. The next daemon cycle resumes its durable
cursor. A row appended after a historical fallback starts is outside that
fallback's frozen `scanBoundary`; it can require **one extra append-drain pass**
even when its observation time sorts before the historical cursor.

For a stable session with `N` eligible and skipped rows to scan, the cycle
proof uses this load-aware pass bound:

`ceil(N / 5,000) + floor(T / 250 ms) + 1`

`T` is the sum of measured summary-update durations through completion. A
nonfinal pass must be charged to its row cap, its time cap, or the single
append-drain allowance. The proof checks this condition on every partial pass,
checks that each session finishes within the resulting bound, and compares the
sent wire to a full rebuild. Its slow-read run adds latency to each row as the
summarizer consumes it and asserts that the 250 ms cap actually binds. The `+1` is the
append-drain allowance and also covers a final empty read when the row count
lands exactly on a cap.

This is a measured bound for an uninterrupted, progressing scan, not an
unconditional promise of `ceil(N / 5,000) + 1` cycles. A ledger mutation or a
future row entering the horizon starts a new rebuild. A repeated zero-progress
read timeout has no finite drain bound until reads recover; it must be reported
as pending, never as caught up or sent.
