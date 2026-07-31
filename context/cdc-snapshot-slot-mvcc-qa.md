# CDC, snapshots, replication slots & MVCC — Q&A reference

A working Q&A session clarifying the Postgres/pgcopydb fundamentals behind the
migration's CDC phase (#26) and the parallelism/consistency trade-offs. Captured
for future reference. Answers were deliberately terse; expanded lightly here.

---

**Q: "Parallelism is identical everywhere" for CDC — fundamental to pgcopydb, or could a custom solution be more fine-grained?**

Partially fine-grainable. *Receive* is fundamentally serial — one slot = one ordered
WAL stream (a Postgres constraint, not pgcopydb's). *Apply* could be parallelized in
a custom solution: non-conflicting transactions applied concurrently while preserving
per-key order.

**Q: Creating one slot — pgcopydb or Postgres constraint?**

pgcopydb's choice. Postgres allows many logical slots, each with its own publication
filtering a table subset. So you *could* shard CDC across N slots+publications. The
caveat is cross-shard transaction atomicity/ordering is lost.

**Q: One example of inconsistency from multiple slots over disjoint table subsets?**

A source transaction inserts `orders` (slot A) + `order_items` (slot B). The two are
independent streams that apply at different speeds and can crash independently → the
target can hold a child row before its parent (FK violation), or an order with no
items (a torn transaction).

**Q: If we relax FK/PK and apply them just before cutover, can we still be inconsistent at the very end?**

No, if done right: freeze source writes at cutover and drain *every* slot to the same
final LSN → the target converges to the source's exact end state, and deferred
constraint validation then passes. Yes, if slots stop at different LSNs (torn final
state).

**Q: What is an LSN?**

Log Sequence Number — a monotonic pointer to a byte position in Postgres's WAL
(write-ahead log). Every change gets one; it's how you say "resume/replay from exactly
here." Format like `E/D00090F0`.

**Q: LSN related to snapshot_id?**

Related, not the same. An LSN is a *position* in the WAL (a "when"). A snapshot_id
names a *visibility set* — which transactions count as committed at that instant. Slot
creation pins both: the same moment from two angles (byte-position vs row-visibility).

**Q: Custom CDC per shard tied to the same tables the shard copied, freeze source long enough for all CDC pods to drain, then apply constraints — valid for consistency?**

Yes, valid. Disjoint tables → no cross-shard apply-order hazard; freezing the source
removes torn transactions; draining every slot to the final LSN + deferred constraints
→ target equals source. Cost: the freeze is real downtime.

**Q: Create PKs/FKs without validation as data comes, then enable validation (quick) once CDC finishes — right?**

Only FKs (and CHECKs). PK/UNIQUE have no `NOT VALID` — they're backed by a unique index
that *must* enforce as rows land (build the index, then `ADD CONSTRAINT ... USING
INDEX`). Also FK `VALIDATE` is a full-table scan (lock-light, but not instant on big
tables).

**Q: When recommend (1) single slot + CDC applied after constraints (shorter downtime) vs (2) parallel CDC, multiple slots + apply constraints after source stops writes?**

- (1) Single-slot, online: when a single consumer can keep up with the source's write
  rate → near-zero downtime. The default choice.
- (2) Parallel, freeze-window: when write throughput exceeds one serial apply (lag never
  converges) and you can afford a downtime window.

**Q: Any pgcopydb CLI parameter/sequencing combo that achieves option (2)? (pgcopydb seems fully option 1.)**

Not built-in, but assemblable. No single command shards CDC; you'd run N independent
`pgcopydb follow`/`stream` instances, each with its own `--slot-name` + disjoint
`--filters` + own `--dir`, then coordinate a common `--endpos` to drain all before
cutover.

**Q: pgcopydb parallelizes a lot in one process — only for the main copy, then CDC reverts to a single stream?**

Correct. The heavy parallelism (`--table-jobs`, `--index-jobs`, table splitting) is the
initial copy. CDC is a fixed 2-stage pipeline (receive→apply) over one slot —
pipelined, but no data-parallelism; throughput is one serial stream.

**Q: Relationship between snapshot, publication, and replication slot — and each's role in migration?**

The three pin the **same instant** (slot creation) from different angles:

- **Replication slot** — the *stream + retention*. A named, persistent marker that stops
  Postgres recycling WAL past its start LSN until consumed. A logical slot decodes WAL
  into row-level changes via an output plugin (pgoutput). One slot = one ordered stream
  with one `confirmed_flush_lsn`.
- **Snapshot** — the *consistent read*. Creating a logical slot exports a snapshot at
  exactly the slot's start LSN: a frozen visibility set (which transactions count as
  committed) at that instant.
- **Publication** — the *what*. Declares which tables/operations are in scope; pgoutput
  uses it to filter which changes get decoded. Orthogonal to the other two.

Roles in sequence:
1. Create slot → exports the snapshot *and* begins retaining WAL from LSN X.
2. Initial copy reads every table *using the exported snapshot* → consistent bulk load
   as of LSN X, unaffected by concurrent writes.
3. Publication defines the table set the slot streams.
4. CDC consumes the slot from LSN X onward, decoding published-table changes and applying
   them to the target → catches up everything since the snapshot.

Key guarantee: the slot retained WAL from *exactly* the snapshot's LSN, so there's **no
gap** between "what the copy saw" and "where CDC starts."

**Q: What does the exported snapshot mean in practice — how do pgcopydb/pg_dump know what to copy efficiently? (WAL is for CDC; what does Postgres provide for the bulk copy?)**

Each parallel reader runs `SET TRANSACTION SNAPSHOT <id>`, so all connections see one
MVCC view of the live heap → consistent, lock-free parallel `COPY`. Bulk = MVCC snapshot
+ `COPY`; WAL is only for CDC.

**Q: What is an MVCC view?**

Multi-Version Concurrency Control. Postgres keeps multiple versions of each row. A
snapshot = which versions were committed at instant T; a reader sees only that frozen
set, while writers keep adding new versions without blocking it.

**Q: Is there a process to clean up old versions? Does a replication slot pin versions until released?**

Right. VACUUM/autovacuum reclaims dead row versions no longer visible to any snapshot. An
open held snapshot pins an *xmin* that blocks that cleanup (→ bloat) until its connection
closes. The slot separately pins WAL (and, for logical slots, a catalog_xmin).

**Q: Is a snapshot always necessary when copying — does Postgres create one if not provided?**

Yes. Every statement runs under a snapshot — Postgres always makes an implicit one.
Exporting/sharing a snapshot isn't about *having* one; it's about making *multiple*
connections agree on the *same* view for a consistent parallel copy.

**Q: Ways to explicitly create a snapshot in Postgres — just on create replication slot?**

Two ways. `pg_export_snapshot()` — a function inside a REPEATABLE READ/SERIALIZABLE
transaction (what parallel pg_dump uses); and `CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT`
on the replication protocol. Both consumed via `SET TRANSACTION SNAPSHOT`.
