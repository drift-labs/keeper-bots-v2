## Goal
Refactor `src/bots/liquidator.ts` from a single-process polling loop into a horizontally scalable, event-driven service using Redis Streams, following the design in `src/bots/liquidatorV2/architecture.md`.

## Target Runtime Topology
- Single Node.js service binary with role-based behavior (via `ROLE` env):
  - Coordinator (leader): holds one `DriftClient`, `UserMap`, price subscriptions; runs Watcher + Scanner; prepares liquidation transactions.
  - Workers (followers): no Drift subscriptions; only read prepared tx jobs and send transactions.
- Redis Streams provide the queues; KEDA scales workers on stream lag.

## Modules and Responsibilities
1) watcher.ts
- Subscribe to price/oracle updates (prefer websocket via existing SDK subscribers or Pyth).
- Detect assets with significant moves or risk thresholds.
- Map assets → at-risk users (via in-Redis inverted index `atRisk:{asset}` or on-demand lookup through `UserMap`).
- Publish candidate events to `liq:candidates` (`XADD MAXLEN ~ <N>`).
- Event payload: `{ userPubkey, authority, affectedMarkets, reason, ts, slot, idemKey }`.

2) scanner.ts
- Periodically shard and sweep users using `UserMap` (held in coordinator) to catch missed cases.
- Emit the same candidate schema to `liq:candidates`.
- Runs only on coordinator (guarded by leader election lock).

3) coordinator.ts
- Runs only in coordinator role.
- Consumes `liq:candidates` (consumer group `coord`) with small concurrency.
- For each candidate:
  - Acquire `lock:liq:{userPubkey}` (SET NX PX TTL) to prevent parallel planning for the same user.
  - Recompute health using the single in-process `DriftClient` + `UserMap`.
  - If liquidatable, decide action (perp PnL, perp pos, spot borrow) using logic factored from `liquidator.ts`.
  - Build transaction(s): derive Ixs, lookup tables, CU limit/price (simulate to size), bundle into a Versioned Transaction Message (unsigned).
  - Publish a tx job to `liq:txs` with all data needed by workers to sign+send (see Tx Job Schema).
  - Release user lock.
- On error, retry within bounded attempts; after budget, DLQ original candidate.

4) workers.ts
- Consumer group `workers` on `liq:txs`.
- For each tx job:
  - Enforce idempotency using job `idemKey` (skip if seen).
  - Rebuild Versioned Transaction from provided message + LUTs; sign with local wallet; send using configured strategy (priority fees supported by provided CU price in job).
  - Report result to `liq:results`; `XACK` on success; on final failure, add to `liq:deadletter`.
- Recover stalled messages via `XAUTOCLAIM`.

5) queue.ts
- Redis Streams and keys utility:
  - ensureStreamAndGroup(stream, group)
  - xaddCandidate, xreadCandidates, xackCandidate
  - xaddTxJob, xreadTxJobs, xackTxJob
  - xaddResult, xaddDeadLetter, xautoclaim

6) leader.ts
- Redis-based leader election using `SETNX lock:leader` with periodic renewal.
- Starts Watcher, Scanner, and Coordinator when leadership is held; stops them on loss.

7) health.ts / metrics.ts
- Prometheus metrics: stream lag, XPENDING per group, oldest pending age, coordinator build latency, sim success rate, worker send latency, success/fail, retry counts, RPC latency.

8) main.ts (bootstrap)
- If `LIQ_V2=1`:
  - Initialize Redis, leader election, and based on `ROLE` start Coordinator stack or Worker loop.
  - Coordinator creates one `DriftClient`, `UserMap`, `PriorityFeeSubscriber`, lookup tables.
- Otherwise, run legacy `liquidator.ts` path.

## Code Extraction From current liquidator.ts
Extract pure logic and tx-builders; remove intervals/mutexes:
- Perp liquidation: `liqPerp`, `calculateBaseAmountToLiquidate`.
- Perp PnL: `liqPerpPnl` (settle, borrow-for-pnl), `calculateClaimablePnl`.
- Spot: `findBestSpotPosition`, `liqBorrow`, `liqSpot`, Jupiter/Drift spot close helpers.
- CU/fee plumbing: `simulateAndGetTxWithCUs`, `PriorityFeeSubscriber` handling.
- Replace local mutexes with Redis locks in coordinator only; workers do not lock users.

Suggested new structure under `src/bots/liquidatorV2/`:
- `core/actions.ts`: construct Ixs and Versioned Tx Messages (no signing).
- `core/decision.ts`: choose liquidation strategy for a user snapshot.
- `core/state.ts`: snapshot user + market state, compute health/eligibility.
- `core/tx.ts`: simulation/CU sizing, LUT collection helpers.
- `redis/queue.ts`: streams + keys helpers.
- `redis/locks.ts`: leader and user locks.
- `roles/watcher.ts`, `roles/scanner.ts`, `roles/coordinator.ts`, `roles/workers.ts`, `roles/leader.ts`.
- `main.ts`: role router (V2).

## Streams and Schemas
1) Stream: `liq:candidates`
- id: Redis Stream ID
- field `data` (JSON):
  - userPubkey: string
  - authority: string
  - affectedMarkets: { spot: number[], perp: number[] }
  - reason: 'price-move' | 'periodic-scan'
  - slot: number
  - ts: number
  - idemKey: string (userPubkey+slot)

2) Stream: `liq:txs` (prepared transactions for workers)
- field `data` (JSON):
  - idemKey: string
  - userPubkey: string
  - action: 'perp' | 'perp-pnl' | 'spot'
  - marketIndex: number | null
  - versionedMessage: base64 (serialized VersionedMessage; unsigned)
  - lookupTables: string[] (LUT pubkeys as base58 if external fetch needed)
  - cuLimit: number
  - cuPriceMicroLamports: number
  - sendOpts: { skipPreflight?: boolean; maxRetries?: number }
  - telemetry: { slot: number; ts: number }

3) Stream: `liq:results`
- field `data` (JSON): `{ idemKey, txSig, status: 'ok' | 'error', error?: string, durationMs }`

4) Stream: `liq:deadletter`
- field `data` (JSON): original candidate or tx job + `{ error, retries, ts }`

## Redis Keys
- `lock:leader` → value: consumerId, TTL: 10s; renewed every 3s.
- `lock:liq:{userPubkey}` → TTL: short (e.g., 20s), held only by coordinator during planning.
- `idem:{idemKey}` → TTL: 2-5 min; set by workers upon successful send.

## Concurrency & Backpressure
- Coordinator: small batch `xreadgroup` (COUNT 8-16) to avoid starving workers; planning concurrency 4-8.
- Workers: batch read COUNT 16-64, BLOCK 2000ms; `p-limit` for tx sends (2-4).
- Circuit breakers for RPC providers; retry budget aligned with existing config.

## Observability
- Coordinator: candidate→job conversion rate, sim success rate, plan latency, user lock contention.
- Workers: send latency, land rate, errors by code, idempotent skips.
- Streams: lag, XPENDING, oldest pending age.

## Local Dev Flow
- docker-compose starts: redis, one coordinator (leader), one or more workers.
- Provide `.env.demo` (RPC, keys), `config.yaml` for Drift config.
- Scale workers: `docker compose up --scale liquidator-worker=3`.

## Migration Plan
1) Create `src/bots/liquidatorV2/` skeleton modules above.
2) Extract tx-building logic from `liquidator.ts` into `core/*` (no signing, no intervals).
3) Implement coordinator role to consume candidates and produce tx jobs.
4) Implement worker role to consume tx jobs, sign and send, and write results.
5) Add `index.ts` V2 entry routing: if `LIQ_V2=1` then start role per `ROLE` env; else legacy path.
6) Ensure Redis initialization: create streams and consumer groups (`XGROUP CREATE liq:candidates coord $ MKSTREAM`, `XGROUP CREATE liq:txs workers $`).
7) Phase test: coordinator-only dry-run (plan/no send) → enable one worker → scale out workers.
8) When stable, disable legacy `liquidator.ts` loop for deployments and use V2 roles.

## Risks & Mitigations
- Double creation/sending: coordinator user locks + worker idem keys.
- Coordinator overload: keep planning concurrency small; offload heavy work to workers.
- RPC flakiness: reuse existing multi-endpoint senders and priority fee subscriber; include cuPrice in jobs.
- Stream growth: `MAXLEN ~ N`, DLQ policy, dashboards on lag and pending ages.
