# Issue #23: why the snapshot holder is a thin pgcopydb wrapper + some custom Python

**Question:** issue [#23](https://github.com/berenguel/pgcyan/issues/23) says
*"Prefer driving pgcopydb (thin wrapper) over hand-rolled
`CREATE_REPLICATION_SLOT` SQL."* Given that, why does the implementation
(`pg_cyan/snapshot.py`, `PgCyanSnapshot`) contain any custom Python at all?

**Short answer:** the slot + snapshot creation *is* pgcopydb-driven, exactly as
the ticket asks — the custom Python only covers the things pgcopydb's `snapshot`
subcommand does **not** do: create the publication, emit the required output
artifacts, and hold + gracefully shut down the connection.

---

## 1. The slot + snapshot are created by pgcopydb, not by hand

The core of the module shells out to:

```
pgcopydb snapshot --follow --plugin pgoutput --slot-name <slot> --dir <work-dir>
```

`--follow` is what makes pgcopydb issue the replication-protocol command
`CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput RESERVE_WAL true SNAPSHOT
'export'`, reserve WAL, export the consistent snapshot, write the snapshot id to
`<work-dir>/snapshot`, and then hold the connection open until signalled.

This is deliberately **not** hand-rolled. Getting WAL reservation + snapshot
export atomically correct by hand is precisely where you silently lose data (the
pgcopydb source even comments on this: using a `pg_export_snapshot()` snapshot to
later create the slot "creates a situation where we miss data, probably because
the Postgres system doesn't know how to reserve the WAL to decode properly").
That is the failure mode the ticket's "prefer pgcopydb" guidance is protecting
against, and we honour it.

## 2. What pgcopydb's `snapshot` path does NOT do (verified in-source)

Checked against the actual fork (`projects/pgcopydb`), not assumed:

- **Publication creation.** The `snapshot` subcommand's option parser
  (`cli_snapshot.c`) never sets `publicationAutoManaged` and exposes no
  `--publication` flag. Its pgoutput path guards publication creation on
  `slot->publicationAutoManaged` (`snapshot.c`), which is false there — so
  `pgcopydb snapshot --follow` creates the **slot and snapshot but not the
  publication**. Publications are only auto-managed on the `clone --follow` /
  `stream setup` paths.

  So the holder creates the publication itself: a single
  `CREATE PUBLICATION <name> FOR ALL TABLES`, composed via `psycopg2.sql`. Note
  this is **not** the `CREATE_REPLICATION_SLOT` SQL the ticket cautions
  against — it is ordinary DDL with no WAL/snapshot atomicity subtlety, and
  there is no pgcopydb sub-command that creates just a publication standalone.

## 3. What else the custom Python provides

None of the following is emitted by pgcopydb; all are required by the ticket or
the repo's conventions:

- **The two output artifacts** `source-pg-slot.json` (slot + snapshot name +
  captured `consistent_point`) and `source-pg-publication.json` (publication
  name), in the repo's success-vs-error two-file shape.
- **`consistent_point` capture** via a read-only
  `SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = ...`.
- **Hold + graceful shutdown** — supervise the pgcopydb child, wait for the
  snapshot to land, and on SIGTERM/SIGINT forward the signal so pgcopydb closes
  the replication connection cleanly. This is the ticket's "prevent stale
  connections / collateral effects on graceful exit" requirement.
- **Fit the `PgCyan<Stage>` orchestrator pattern** (`_log_event`, graceful
  degradation — `establish()` never raises, `--plan-file` override so the
  planner (#38) can supply publication/slot/mode).

## 4. The alternative we rejected, and why

We could push the publication step into pgcopydb by switching the driver from
`snapshot --follow` to a `stream setup`-style path, which *does* auto-manage the
publication. Rejected because `stream setup` also initialises replication
origin / replay-DB state — and #23 explicitly says **"Do not create or advance
any replication origin."** That work belongs to the follow-CDC phase (#4), not
to this phase-1 holder. Keeping `snapshot --follow` + an explicit
`CREATE PUBLICATION` is the minimal shape that satisfies both constraints.

---

**Bottom line:** pgcopydb owns the hard replication-protocol work (slot +
snapshot); the Python is a thin supervisor that adds the one thing pgcopydb's
snapshot path omits (the publication) plus the artifact / hold / shutdown glue
the ticket asks for. This complements [issue #22's assessment](./issue-22-migration-stage-assessment.md),
which reached the same "don't hand-build what pgcopydb already does" conclusion
for the publication ask that #23 absorbed.
