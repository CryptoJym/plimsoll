# 0037 — Session sync: the workspace holds REAL session rows that join to their events

## TL;DR
- **Problem**: Hosted `AiWorkSession` table was empty (0 rows) while the ledger successfully stitched sessions for every event. This broke per-session and per-person analytics (cloud #24 Phase D3/D4).
- **Solution**: The collector now pushes one SNAPSHOT per stitched session with `kind: "session_sync"`. The cloud service upserts these into the workspace using a deterministic session ID.
- **Strategy**: Grow-only upserts to preserve historical analytics.
- **Result**: Live run on the real workspace now holds **4,185 session** rows.

## Implementation Details

### Data Flow
1.  **Collector**: Detects a completed stitched session.
2.  **Payload**: Sends a payload to the ingest route: