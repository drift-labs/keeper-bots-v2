# 🏗️ Node.js Liquidator Bot – Current Proposed Architecture (Redis Streams)

## Overview

The liquidator system is a **Node.js-based service** designed to monitor user health, detect liquidation opportunities, and execute liquidations efficiently. It runs on **AWS EKS** and uses **Redis Streams** for queueing and **KEDA** for event-driven autoscaling.

The system is architected to stay lightweight during calm market periods but scale out rapidly when volatility increases and many users become liquidatable.

---

## 🧱 Core Components

### 1. **Watcher (Price/Signal Producer)**

* Subscribes to on-chain or oracle price feeds (WebSocket preferred).
* Detects significant market movements or risk threshold breaches.
* Looks up users exposed to affected assets via Redis inverted index (`atRisk:{asset}` → set(userIds)).
* **Publishes candidate events** to Redis Stream `liq:candidates` using `XADD` (with `MAXLEN ~ N` for bounded retention).
* Runs only on the **leader pod** (elected via Redis lock).

### 2. **Scanner (Periodic Sweep)**

* Shard-based background process that periodically re-checks all users.
* Ensures no liquidatable user is missed during quieter periods.
* Pushes candidate events to the same Redis Stream `liq:candidates`.
* Also runs only on the **leader pod**.

### 3. **Workers (Liquidation Executors)**

* All pods run workers that consume from Redis Streams using **consumer groups**.
* Consumer group: `workers` on stream `liq:candidates`.
* For each candidate user:

  * Acquire distributed lock (`lock:liq:{userId}`) in Redis.
  * Recalculate user’s health factor using cached + fresh on-chain reads.
  * If liquidatable:

    * Execute liquidation transaction (RPC call).
    * Ensure **idempotency** via Redis key (`idem:{userId}:{block}`).
  * `XACK` on success; push to `liq:deadletter` after retry budget.

Workers handle concurrency limits:

* `p-limit` for read concurrency.
* Separate limit for transaction submission.
* Circuit breakers for RPC providers (e.g., `opossum`).
* Use `XAUTOCLAIM` or `XCLAIM` to recover stalled messages.

---

## ⚙️ Infrastructure

### **Kubernetes (EKS)**

* Single **Deployment** containing the same Node.js service.
* **Leader election** ensures only one pod runs Watcher and Scanner.
* All pods run Workers concurrently.
* **KEDA** handles autoscaling based on Redis Streams lag.

### **Redis (Streams + Keys)**

* In-cluster **StatefulSet** (via Helm or Operator) with persistent volumes.
* Streams:

  * `liq:candidates` → primary work stream
  * `liq:deadletter` → poison/unrecoverable events
* Consumer group: `workers` (created with `XGROUP CREATE liq:candidates workers $ MKSTREAM`).
* Persistence: enable **AOF** (appendonly) for durability; set sensible `MAXLEN ~` on streams.
* Security: use Redis **ACLs** and Kubernetes Secrets for credentials.

### **KEDA Autoscaling**

* Uses the **redis-streams** trigger.
* Scales the liquidator Deployment between 1 and 100 pods based on:

  * **Pending entries** per group (`XPENDING`), and/or
  * **Age of the oldest pending** entry.

### **RPC & On-chain Interaction**

* Bounded concurrency with connection pools.
* Circuit breakers and fallback RPC providers.
* Batching via multicall when supported.
* Full pre-check before sending tx to avoid reverts.

---

## 🧩 Node.js Process Layout

Each pod runs the same service binary:

* **Leader pod**: runs Watcher + Scanner + Workers.
* **Follower pods**: run Workers only.

### Key modules:

* `watcher.ts` → Subscribes to prices, `XADD` candidate events to `liq:candidates` with dedupe fields.
* `scanner.ts` → Periodic shard scan of users, `XADD` candidates.
* `workers.ts` → `XREADGROUP GROUP workers <consumer>` to fetch jobs; `XAUTOCLAIM` for stalled; `XACK` on success; on failure after retries, `XADD` to `liq:deadletter`.
* `queue.ts` → Redis Streams producer/consumer utilities.
* `main.ts` → Bootstraps components, performs leader election.

---

## 🧠 Design Principles

| Goal                       | Mechanism                                                                            |
| -------------------------- | ------------------------------------------------------------------------------------ |
| **Event-driven**           | Watcher/Scanner publish to Redis Streams, Workers consume via consumer groups        |
| **Backpressure safety**    | Stream lags absorb spikes; lag drives autoscaling via KEDA                           |
| **Idempotent + Safe**      | Redis keys prevent duplicate txs or concurrent liquidations                          |
| **Horizontal scalability** | Stateless workers, KEDA-driven scaling                                               |
| **Fault isolation**        | Optionally separate node pool for Redis; worker crashes don’t affect persistence     |
| **Observability**          | Prometheus metrics: stream lag, success rate, RPC latency, pending age, worker count |

---

## 🖥️ Node Placement

* **Redis** runs as a **StatefulSet** with persistent volumes. Prefer a small dedicated node pool (tainted + labeled) for IO stability; acceptable to co-locate in dev.
* **Liquidator pods** run on general-purpose nodes that scale dynamically.
* Storage classes for Redis persistence (e.g., gp3 with provisioned IOPS/throughput or fast local NVMe with replication considerations).

---

## 🔒 Reliability & Resilience

* **Leader election** (Redis-based) ensures single producer of price/scan events.
* **Locks + TTLs** guarantee no double liquidation.
* **At-least-once semantics** via Streams + `XACK`.
* **Recovery**: `XPENDING` inspection and `XAUTOCLAIM` reclaim stalled messages; DLQ via `liq:deadletter`.
* **PDBs & anti-affinity** for Redis pods (3 replicas with Sentinel/Operator if aiming for HA).
* **Graceful shutdown**: drain consumers, `XACK` in-flight before SIGTERM completes.

---

## 🚀 Next Steps

* [ ] Helm install Redis (or Operator), enable AOF, set ACLs/Secrets.
* [ ] Bootstrap stream & consumer group (`XGROUP CREATE liq:candidates workers $ MKSTREAM`).
* [ ] Add KEDA `ScaledObject` with `redis-streams` trigger and thresholds.
* [ ] Instrument Redis Streams metrics (INFO, `XPENDING`, oldest age) into Prometheus/Grafana.
* [ ] Tune worker batch size, `block` timeouts, retry policy, and DLQ handling.
* [ ] Document disaster recovery & retention (MAXLEN, RDB/AOF settings).

---

*Last updated: 2025-10-04*
