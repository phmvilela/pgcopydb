# pgcyan — migration architecture and decision tree

Design notes for the migration engine. Scope: online (near-zero-downtime) PostgreSQL
migrations where the target is a new instance — region, SKU, storage, network topology,
tenant, or major version change.

---

## 0. The one thing to internalise

**Every online migration here is logical replication.** `pg_dump`/`pg_restore`, pgcopydb,
and `copy_data = true` are not alternatives to it — they are three implementations of
*the initial load*. All three hand off to the same slot, the same `pgoutput` stream, the
same subscription.

There are exactly two variables:

| Variable | Values | Decided by |
|---|---|---|
| **Load mechanism** | `copy_data=true` \| pgcopydb \| `pg_dump -Fd -j` \| custom COPY | load duration, resume, index deferral |
| **N** — subscription groups | 1 \| >1 | whether apply throughput converges |

Everything else — exported snapshot, no origin advance, sequence resync at cutover,
large-object refusal — is **invariant**. Not a mode, not a flag. Core.

---

## 1. Decision tree

```
                    ┌─────────────────────────────┐
                    │ pg_largeobject_metadata > 0 │
                    └──────────────┬──────────────┘
                          yes      │      no
                  ┌────────────────┘      └────────────────┐
                  ▼                                        ▼
        ┌──────────────────┐                  ┌──────────────────────────┐
        │ REFUSE online.   │                  │ Can a physical read      │
        │ No online path   │                  │ replica reach the target?│
        │ exists. Offline  │                  │ (same major version,     │
        │ dump/restore, or │                  │  replica supports the    │
        │ freeze LO writes │                  │  target's topology)      │
        └──────────────────┘                  └────────────┬─────────────┘
                                                  yes      │      no
                                          ┌────────────────┘      └──────────┐
                                          ▼                                  ▼
                              ┌───────────────────────┐        ┌──────────────────────────┐
                              │ READ REPLICA + PROMOTE│        │ Does apply converge?     │
                              │ Strictly better:      │        │ apply_rate >             │
                              │  no PK requirement    │        │   src_write_rate ×       │
                              │  DDL replicated       │        │   (1 + load_days /       │
                              │  sequences carried    │        │        catchup_days)     │
                              │  LOs carried          │        └────────────┬─────────────┘
                              │  no apply ceiling     │             yes     │     no
                              │  no WAL retention     │       ┌─────────────┘     └───────────┐
                              └───────────────────────┘       ▼                               ▼
                                                    ┌──────────────────┐         ┌────────────────────────┐
                                                    │ N = 1            │         │ N > 1                  │
                                                    │ single group     │         │ split by FK components │
                                                    │ single slot      │         │ N slots, N snapshots,  │
                                                    │ pgcopydb load    │         │ N subscriptions        │
                                                    └──────────────────┘         └────────────────────────┘
```

**Size is not a node in this tree.** A 32TB warehouse with low write rates converges on
N=1. A 3TB OLTP source at 80 MB/s of WAL may not. The gate is the convergence
inequality, not bytes.

---

## 2. Load mechanism — problems with each

### `copy_data = true` (native tablesync)

*Correct by construction, untunable.*

- ✅ No exported-snapshot handling. Per-table LSN (`pg_subscription_rel.srsublsn`),
  per-table temporary slot, per-table snapshot. Overlap is impossible.
- ✅ **`xmin` advances continuously** — each tablesync releases its snapshot when its
  table finishes. No global hold. This is the one thing it does better than everything
  below, and on a multi-day load it is not nothing.
- ❌ **Cannot defer index builds.** The PK must exist before tablesync completes (it *is*
  the replica identity, and apply starts on that table the moment sync ends). Every row
  of the copy pays full index maintenance. On a 5TB table with 6 indexes this is the
  difference between finishing and not.
- ❌ **No resume.** A tablesync that dies at hour 19/20 truncates and restarts that table.
  Over a 7-day load, something dies.
- ❌ **No intra-table parallelism.** One worker per table. An 8TB table is one worker,
  forever. `max_sync_workers_per_subscription` (default **2**) doesn't help.
- ❌ **No progress reporting.** A state letter, not a percentage. Useless for a 7-day ETA.

**Verdict:** correct, and unusable above a few hundred GB. Keep it as a supported mode
for small sources — it is the only mode with zero snapshot machinery, and therefore the
only one that can't be got wrong.

### `pg_dump -Fd -j` / `pg_restore -j`

- ✅ Parallel dump and restore; deferred index/constraint builds via `--section`.
- ✅ Artifacts on disk survive a crash — retryable.
- ❌ **Requires an exported snapshot**, which requires a `replication=database` session
  held open and *idle* for the entire dump. Any command on that connection invalidates
  the snapshot. So does a TCP reset. Set `tcp_keepalives_idle` or Azure's LB will drop it
  at ~4 minutes.
- ❌ **One `xmin` for the whole dump.** Autovacuum cannot reclaim anything on the source
  for the duration. On a multi-day load this is a production incident, not a nuisance.
  (Note: `pg_dump` holds this `xmin` anyway, with or without `--snapshot`. The exported
  snapshot adds only the slot-creation → dump-start gap, which is seconds if scripted.)
- ❌ **No intra-table parallelism.** `-j` is per-table. A single 8TB table is one worker.
  Same wall as tablesync.
- ❌ Table-level `-t` filtering is clumsy at 2000 tables. Generate the list.

### pgcopydb

- ✅ Exported snapshot handled for you (`pgcopydb snapshot` holds it in a separate
  process — the fragile-session problem, solved).
- ✅ **`--split-tables-larger-than`** — CTID-range sharding across workers. The only
  off-the-shelf answer to the single-huge-table wall.
- ✅ Deferred index builds, parallel, with `--resume` / `--not-consistent`.
- ✅ Progress reporting.
- ❌ Still **one global snapshot** for the clone → same `xmin` hold as `pg_dump`. Its
  answer is to be fast enough that the window is hours, not days.
- ❌ **`--follow` gives you one stream and one apply process.** Cannot produce the N>1
  topology. For apply-bound sources it hits the same ceiling.
- ❌ Release cadence. Judgment call — but it's the load path, not the consistency path,
  so a stale-but-working loader is a lower risk than a hand-rolled new one.

### Custom parallel COPY

- ✅ Full control: CTID sharding, per-group snapshots, index deferral, progress, resume.
- ❌ **You are rebuilding pgcopydb.** Parallel COPY, range sharding, retry, index/constraint
  ordering, progress accounting — all solved elsewhere. Pure cost, zero differentiation.
- ❌ It is in the **consistency path**. Every bug here is a silent data bug.

**Recommendation: start on pgcopydb behind an interface.**

```
load(snapshot_handle, table_set, target) -> populated_target
```

Swapping to `pg_dump` or a custom loader later becomes a config change, not a rewrite.
This seam is also where your tests live: any loader that satisfies the contract can be
run against the same consistency assertions.

---

## 3. Topology — N=1 vs N>1

### The constraint nobody plans for

**Apply is single-threaded per subscription.** One worker on the target replays the
source's *entire* write stream, serially, in commit order. `streaming = parallel` (PG16+)
only parallelises *within* large in-progress transactions — it does nothing for a stream
of many small OLTP transactions, which is what actually saturates apply.

If apply cannot sustain the source's write rate, **you never converge.** There is no
cutover date. Lag grows forever. This is the most common way a large migration dies, and
it is invisible until day 3.

### Convergence budget — compute this before starting

```
required_apply_rate  >  src_write_rate × ( 1 + load_days / acceptable_catchup_days )
```

You start day 1 already owing a `load_days`-sized backlog. Apply must beat the source's
steady-state rate *plus* burn that debt down.

**If the inequality fails, more tuning will not save it.** The levers are, in order:
1. Shrink `load_days` (faster load) — the debt is proportional to it.
2. Shrink `src_write_rate` — exclude churn tables from the publication (§5).
3. Raise `required_apply_rate` — index deferral, target HA off, N>1.

pgcyan should **refuse to start** a migration whose budget doesn't close, and say which
lever to pull. This is the single most valuable thing the planner does.

### N>1 — the shape

```
for each group i in 1..N:
    CREATE PUBLICATION pub_i FOR TABLE <disjoint table set i>
    CREATE_REPLICATION_SLOT slot_i LOGICAL pgoutput (SNAPSHOT 'export')  -> snap_i
    load(snap_i, table_set_i, target)
    CREATE SUBSCRIPTION sub_i ... slot_name=slot_i,
                                  copy_data=false, create_slot=false,
                                  disable_on_error=true, binary=true
```

**Each `(slot, snapshot, load, subscription)` is one atomic unit.** You cannot create N
slots and take one dump:

- Dump at the **earliest** slot's snapshot → later slots start *ahead* of the dump →
  commits in between are in neither → **data loss**.
- Dump at the **latest** slot's snapshot → earlier slots start *behind* the dump →
  **overlap → duplicate-key crash loop, or silent row duplication on keyless tables**.

There is no single dump consistent with N slots created at N instants. One snapshot per
group. No exceptions.

Groups are **not** mutually consistent mid-flight. That is fine — nothing reads the target
until cutover, when all N drain to the source's final LSN and the whole thing converges.

### The cost of N — this bounds it

**Every slot decodes the entire WAL stream.** Publication filtering happens in the output
plugin, *after* transaction reassembly. So N slots = N walsenders, each reading and
decoding 100% of the source's WAL and discarding what isn't theirs.

**Source decode CPU scales linearly with N**, and it competes with the customer's
production workload. This is why N is single digits, not dozens. Start at 4, measure
source CPU and per-group apply lag, adjust.

### Grouping algorithm

1. **Build the FK graph.** Compute connected components. Commit order is preserved only
   *within* a subscription — FK-related tables must land in the same group.
   > **Verify on your build:** the apply worker runs with `session_replication_role =
   > replica`, so FK triggers should not fire during apply. If that holds, cross-group
   > FK violations are impossible mid-flight and grouping is freer than stated. Test it
   > before relying on it.
2. **Rank components by write rate** (`n_tup_ins + n_tup_upd + n_tup_del` from
   `pg_stat_user_tables`, or better, sampled WAL volume). Apply is the bottleneck, so
   balance on *write rate*, not bytes.
3. **Isolate the top write-volume components** into their own groups.
4. **Bin-pack the long tail** into remaining groups, balancing secondary on total bytes
   (so the load phase finishes evenly).

---

## 4. Concrete plan — 32TB, 2000 tables

Assume OLTP, PG16+, HA on, apply-bound (the stated symptom).

**Expected shape of 2000 tables:** ~20 tables hold ~95% of bytes and ~95% of writes. The
other 1980 are small. Do not treat them uniformly.

### Grouping

| Group | Contents | Rationale |
|---|---|---|
| **G1** | The single highest-write FK component | Its own apply worker |
| **G2** | Next highest-write component | Its own apply worker |
| **G3** | Remaining large tables, bin-packed | Balance load time |
| **G4** | The ~1900-table long tail | One worker; negligible write rate |
| **X** | *Excluded* — see §5 | Not replicated at all |

**Start at N=4.** Measure source decode CPU and per-group `behind_apply`. Add groups only
if source CPU has headroom. Every added group taxes the source.

### Sequence of operations, per group

```
1.  CREATE PUBLICATION pub_i FOR TABLE ...
2.  CREATE_REPLICATION_SLOT slot_i ... (SNAPSHOT 'export')   -> snap_i, consistent_point
3.  Pre-create schema on target: tables + PK/unique ONLY.
    NO secondary indexes. NO FK constraints. NO triggers.
4.  load(snap_i, tables_i)  -- pgcopydb, CTID-sharded for tables > ~100GB
5.  CREATE SUBSCRIPTION sub_i (copy_data=false, create_slot=false,
                               slot_name=slot_i, disable_on_error=true, binary=true)
6.  Burn down lag. Monitor behind_apply.
7.  Once lag is stable and small: build secondary indexes, parallel.
    Apply stalls on each table during its index build (SHARE lock blocks writes);
    lag spikes, then recovers. Budget for this — do it with slack before cutover.
8.  ADD FK constraints NOT VALID, then VALIDATE CONSTRAINT (non-blocking).
9.  Verify (§6).
```

**Step 3 is the highest-leverage line in this document.** Every secondary index on a table
is an extra random write per replicated row. A table with 6 indexes costs ~7× the apply
I/O of the same table with none. Apply is your bottleneck, and you are choosing to run it
for days against a fully-indexed target if you skip this.

### Target-side settings during catch-up

| Setting | Value | Why |
|---|---|---|
| **HA** | **Disabled during catch-up** | Zone-redundant HA = synchronous commit to standby = a network round-trip in the critical path of your single apply worker, per commit. Re-enable before cutover. Often decisive. |
| Storage | Premium SSD v2, IOPS explicitly provisioned | Apply is random write. A target sized for steady state is undersized for a 7-day firehose. |
| `max_wal_senders` / `max_replication_slots` | ≥ N + headroom | On **both** ends |
| `max_logical_replication_workers` | ≥ N + sync workers | Target |
| `autovacuum` | **More aggressive, not less** | Apply generates dead tuples fast |
| `synchronous_commit` | `off` on the target during catch-up | Apply is a redo stream; a crash means re-replaying from the slot, which is safe. Meaningful win. |

### Source-side, before you start — hard gates

| Check | Failure action |
|---|---|
| `pg_largeobject_metadata` non-empty | **REFUSE.** No online path exists. |
| Tables with no PK/unique and default replica identity | **REFUSE.** These silently duplicate rows on any overlap, and never error. |
| `REPLICA IDENTITY FULL` on a table > ~1M rows | **REFUSE.** Every replicated UPDATE/DELETE becomes a seq scan on the target. Not "less efficient" — non-terminating. |
| `max_slot_wal_keep_size` set | **Warn loudly.** If exceeded, the slot is *invalidated* (`lost`) and cannot be resumed — the entire multi-day migration restarts from zero. Unset = risk a full disk instead. The customer must choose consciously. |
| Projected WAL retention > source free disk | **REFUSE.** `write_rate × window`. If the source disk fills, the source goes **read-only**. Your migration causes a production outage. |
| HA enabled AND source < PG17 | **Warn hard.** A logical slot does not survive an HA failover pre-PG17. Any failover — *including planned maintenance* — during the window destroys the slot and the migration restarts. Over 7 days this is not a small probability. PG17+: create slots with `failover = true`. |
| Unlogged tables in scope | Warn. Not replicated (no WAL). Target starts empty. |
| Materialized views | Warn. Not replicated. Refresh on target — budget the time; on 32TB it can be hours *inside the cutover window*. |
| DDL churn (e.g. `pg_cron` partition creation) | **Warn hard.** DDL is not replicated. Partition-creating cron jobs *are DDL*. Freeze for the window, or pre-create partitions far enough ahead. |
| Extension state outside published schemas (`cron.job`, etc.) | Warn. Migrate separately. |

---

## 5. Exclude the churn — the underused lever

Often **80% of WAL volume comes from 2–3 tables** that don't need CDC semantics at all:
audit logs, event streams, telemetry, session tables.

**Exclude them from the publication entirely.** Load them at cutover with a
timestamp- or ID-bounded `COPY`, or accept a gap if they're genuinely append-only
telemetry. This attacks `src_write_rate` directly — the term that appears on *both* sides
of the convergence inequality. It is frequently the difference between a migration that
converges and one that doesn't, and it costs nothing.

PG15+ row filters and column lists on the publication are the finer-grained version of
the same idea.

---

## 6. Verification — "zero lag" is not verification

Lag was zero in the silent-duplication case too. At 2000 tables this must be automated.

1. **Per-slot drain:** `confirmed_flush_lsn == pg_current_wal_lsn()` on the source, for
   **every** slot. All N must drain.
2. **Apply workers alive:** `pg_stat_subscription.pid IS NOT NULL` for all N. A NULL pid
   means a crash loop.
3. **Row counts, all 2000 tables**, source vs target. This is the only check that catches
   a keyless table that silently doubled.
4. **Checksum sample** on the top-N tables by value: `md5(string_agg(...))` over a
   deterministic ordering, or per-page checksums on a sampled range.
5. **Sequences:** `last_value` on target ≥ source, for every sequence.

---

## 7. Cutover

```
1. Stop application traffic to the source.
2. Wait: for every slot, confirmed_flush_lsn == pg_current_wal_lsn().
3. Load excluded tables (§5) — final bounded COPY.
4. Resync sequences:
     psql SRC -Atc "SELECT format('SELECT setval(%L,%s,true);',
            schemaname||'.'||sequencename, last_value)
          FROM pg_sequences WHERE last_value IS NOT NULL" > seq.sql
     psql TGT -f seq.sql
5. REFRESH MATERIALIZED VIEW (budget this — can be hours on 32TB).
6. Re-enable HA on the target.
7. Verify (§6).
8. ALTER SUBSCRIPTION ... DISABLE; DROP SUBSCRIPTION;  (all N)
9. Drop slots on the source.
10. Flip the CNAME / virtual endpoint.
```

Steps 3 and 5 are the ones that turn a "near-instantaneous" cutover into a two-hour
outage. Eliminate them beforehand or budget them honestly.

---

## 8. Hard rules for the codebase

**These are not configurable.**

1. **`pg_replication_origin_advance` has no caller.** Not behind a flag, not in a
   "recovery" path. There is no correct value to pass it. Advancing to `restart_lsn` is a
   silent no-op (the walsender clamps any LSN below `confirmed_flush` back up). Advancing
   to a later LSN skips commits that were never in the dump — **silent data loss**. The
   dump's snapshot point is not observable as an LSN; that is precisely what
   `EXPORT_SNAPSHOT` gives you and why the origin never needs touching.

2. **No load path that doesn't take a snapshot handle.** If a caller *can* load without a
   snapshot, eventually one will.

3. **`copy_data = false` + one origin per subscription** ⇒ every table in a group comes
   from **one** snapshot. Cannot be mixed. This is the constraint that makes the exported
   snapshot mandatory rather than merely nice.

4. **Large objects ⇒ refuse.** Not a warning. `pg_largeobject` is a catalog and cannot be
   published, in any version. `pg_dump` copies LOs; replication never carries the delta.
   Nothing errors, lag stays zero, and the target diverges for the whole window. It is the
   only failure here that is silent, unrecoverable, and invisible to every health check.

5. **`disable_on_error = true`** on every subscription. Doesn't prevent a conflict, but
   stops the crash loop from pinning WAL until the source's disk fills.

6. **The AI is never in the data path.** No model chooses an LSN, decides to skip a
   transaction, or picks a snapshot. That is deterministic code with tests. LLM in the
   consistency path = a silent hole in someone's ledger.

---

## 9. Where the AI earns its place

**The planner** — this is the product.
- Read `pg_stat_user_tables`, FK graph, index counts, sampled WAL rate → **propose the
  N-way grouping** and justify it.
- Compute the **convergence budget** and tell the user, *before day 1*, whether this
  migration mathematically converges — and if not, which lever to pull.
- Size WAL retention; check `max_slot_wal_keep_size`; project source disk.
- Run every gate in §4 and emit a **risk report a human signs off on.**
- Identify the churn tables worth excluding (§5) and quantify the WAL saved.

**Triage** — apply worker down: read the target log, classify the conflict, explain it in
English, propose remediations *and refuse the unsafe ones*. If a user asks to skip an LSN,
show them what that transaction contained first.

**Not** the load. **Not** the snapshot. **Not** the LSNs.

---

## 10. What pgcyan is actually competing on

Not the replication — that's Postgres. Not the parallel copy — that's pgcopydb.

The product is: **the planner, the N-way apply split, the convergence math, the preflight
refusals, sequence/matview/excluded-table reconciliation, cutover orchestration, endpoint
flip, and post-cutover verification.**

That is everything the field currently does by hand, and gets wrong.
