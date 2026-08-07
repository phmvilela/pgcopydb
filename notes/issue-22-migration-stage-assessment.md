# Issue #22 (and #22–28): does the Migration stage need custom Python?

**Question:** issue [#22](https://github.com/berenguel/pgcyan/issues/22) asks
to update the Python app to connect to the source and run `CREATE
PUBLICATION migration_pub FOR ALL TABLES`, writing the name to
`source-pg-publication.json`. Does this need to be hand-built in
`pg_cyan`?

**Short answer:** no. `pgcopydb` already creates this publication
automatically as part of `pgcopydb clone --follow` (or standalone via
`pgcopydb stream setup`), with a configurable name. More importantly, #22 is
not an isolated ask — it's the snapshot step of a 7-issue sequence (`#22`–`#28`) that,
taken together, hand-builds a native logical-replication pipeline that
`pgcopydb clone --follow` already implements end-to-end, tested, in C.

---

## 1. What `pgcopydb` does for publication creation

Checked against the actual source (`projects/pgcopydb`, v2.x, checked out
locally):

- `src/bin/pgcopydb/snapshot.c` calls `pgsql_create_publication()` automatically
  before the slot/snapshot step, whenever the plugin is `pgoutput` (the
  default).
- `src/bin/pgcopydb/pgsql.c` (`pgsql_create_publication`) builds the
  publication from `pg_tables` directly — an explicit `CREATE PUBLICATION
  "<name>" FOR TABLE <t1>, <t2>, ...` enumerating every table (minus
  system/internal schemas), honoring `--exclude-table`/`--exclude-schema`
  filters. With no filters this is functionally equivalent to `FOR ALL
  TABLES` for every table that exists *at creation time*.
- The name defaults to the slot name (`"pgcopydb"`, `defaults.h`) but is
  fully configurable via `--publication <name>` (`cli_common.c`). Renaming to
  `migration_pub` is a flag, not new code.
- It's lifecycle-managed: created before the snapshot/slot step
  (`publicationAutoManaged`), dropped on cleanup unless an explicit name was
  given (`ld_stream.c`).

**One real semantic gap:** pgcopydb's version is an explicit table list, not
literal `FOR ALL TABLES` — tables created *after* publication creation won't
auto-join it, where native `FOR ALL TABLES` would pick them up. Only matters
if DDL during migration is expected; if schema is frozen going into cutover
(the normal assumption), it's a non-issue.

---

## 2. The bigger picture: `#22`–`#28` reimplement `pgcopydb clone --follow`

Pulling the full issue list (`gh api repos/berenguel/pgcyan/issues`) shows
`#22`–`#28` spell out a hand-built native logical-replication pipeline, one
issue per step of what `pgcopydb clone --follow` already does as a single
orchestrated, resumable command:

| # | Title | Covered by `pgcopydb clone --follow` |
|---|---|---|
| 22 | Create Publication For All Tables | Auto-created (§1 above) |
| 23 | Create Slot With Exported Snapshot | Core mechanism of the tool — slot + `pg_export_snapshot()`, automatic |
| 24 | Pre-Create Target Schema Without Secondary Indexes | `pgcopydb restore pre-data` / built into `clone` |
| 25 | Initial Copy And Replication Setup | The COPY step of `clone --follow` |
| 26 | Start Change Data Capture | `follow`'s receive/apply workers start automatically after initial copy |
| 27 | Monitor Replication Lag | `pgcopydb stream sentinel get --json` exposes write/flush/transform/replay LSNs directly |
| 28 | Build Secondary Indexes And FK Constraints On Target | `pgcopydb restore post-data`, runs concurrently with CDC catch-up |

Sources: `docs/features.rst` ("Change Data Capture" section — receive/apply
worker design), `docs/tutorial.rst` (pre-data/post-data restore split),
`docs/include/stream-sentinel-get.rst` and `docs/include/list-progress.rst`
(lag/progress introspection), plus the `pgsql.c`/`snapshot.c` source read for
§1.

This is pgcopydb's entire reason for existing: one command does snapshot
export, initial parallel copy, deferred index/constraint build, and CDC
handoff — crash-safe and resumable. Building `#22`–`#28` as separate
hand-rolled Python/psycopg2 steps means re-implementing (and re-testing) a
pipeline pgcopydb already ships in C.

---

## 3. Recommendation

1. **Don't implement #22 as a bespoke Python/SQL step.** If a JSON artifact
   is still wanted for audit/visibility (consistent with pgcyan's two-file
   contract pattern — see
   [pgcopydb-and-pgdump-tooling-assessment.md](pgcopydb-and-pgdump-tooling-assessment.md)),
   it should be a thin wrapper shelling out to `pgcopydb stream setup
   --publication migration_pub ...` (or a `psql` one-liner) and reshaping the
   result into `source-pg-publication.json` — not new SQL logic.
2. **Treat `#22`–`#28` as one design decision, not seven independent
   tickets.** Before starting any of them, decide: is Migration
   "orchestrate `pgcopydb clone --follow`" (thin wrapper + monitoring/gating
   logic — consistent with this project's own §3/§4 recommendation in
   [pgcopydb-and-pgdump-tooling-assessment.md](pgcopydb-and-pgdump-tooling-assessment.md)),
   or "hand-build native pub/sub replication step by step"? Right now the
   backlog is scoped for the latter while pgcopydb gives you the former for
   free. Worth raising with whoever scoped `#22`–`#28` before picking any of
   them up.
