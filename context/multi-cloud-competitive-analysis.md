# Multi-cloud competitive analysis
## Managed PostgreSQL reconfiguration and migration

**Question:** where do the hyperscaler platform features end, and what remains
unserved?

---

## 1. The primitive, and what it structurally is not

AWS, Azure, and GCP are converging on the same building block for low-downtime
change: a **staging clone of a production server**, kept in sync by replication,
promoted in a fast switchover. AWS calls it Blue/Green Deployments. GCP has no
equivalent yet. Azure has none today and is expected to ship one.

Two properties of that primitive matter more than any individual limitation:

**It is 1:1.** One source server produces one target server. This is not a v1
constraint that later releases relax — it is the definition of the operation.

**It is a topology clone.** The staging environment inherits the source's shape:
replicas, storage configuration, backup settings, HA posture. Only a small,
explicitly enumerated set of properties can differ. Every generation of the
feature widens that set; none of them changes the 1:1 shape.

Those two facts, rather than any current gap, determine what stays out of scope
permanently.

---

## 2. AWS — the mature implementation

### Trajectory

| Date | Release |
|---|---|
| Nov 2022 | Blue/Green launches for Aurora MySQL, RDS MySQL, RDS MariaDB. **No PostgreSQL.** |
| Oct 2023 | PostgreSQL support (Aurora PostgreSQL, RDS PostgreSQL) via community logical replication, versions 11.21+, 12.16+, 13.12+, 14.9+, 15.4+ |
| Nov 2024 | Physical replication for minor version upgrades; **storage volume shrink**; managed green-storage initialisation |
| Nov 2025 | Aurora Global Database support |
| Jan 2026 | Switchover downtime reduced to typically under five seconds |
| Apr 2026 | RDS Proxy integration, removing DNS propagation delay at switchover |

PostgreSQL support is roughly two years and nine months old, with meaningful
releases about twice a year and no sign of slowing. **The assumption that a v1
ships narrow and stays narrow is not supported by this record.** Planning should
assume Azure follows a comparable cadence once its own feature lands.

Note the November 2024 positioning in particular: multiple database changes —
minor version upgrade, storage shrink, maintenance, instance scaling — applied
in a *single switchover event*. That is the same "reconfigure without repeated
downtime" value proposition, already claimed.

### Replication method

RDS selects the method automatically, and the choice drives the limitation set:

| Source version | Action | Method |
|---|---|---|
| 16.1+, 15.4+, 14.9+, 13.12+, 12.16+, 11.21+ | Major version upgrade | Logical |
| All supported | Minor upgrade, or no upgrade | Physical |

Major version upgrades are unsupported for sources at or below 15.3, 14.8,
13.11, 12.15, and 11.20 — those instances must reach a qualifying minor version
first.

### What it still cannot do (current documentation)

- Change encryption state in either direction
- Cross AWS accounts — blue and green must be in the same account
- Support cascading read replicas, cross-Region read replicas, Multi-AZ DB
  clusters, or CloudFormation
- Under physical replication: any schema change on green, which is strictly
  read-only, or a manual major version upgrade once green exists
- Under logical replication: replicate DDL or DCL, replicate large objects, or
  auto-refresh materialized views. DDL or large-object activity on blue puts
  green into **Replication degraded**, requiring the deployment and all green
  databases to be deleted and recreated.
- Keep up under sustained high write volume. Apply on green is single-threaded;
  AWS's own guidance is to consider DMS instead for major version upgrades with
  high-volume write workloads.

That last item is the significant one: **the vendor documents a throughput
ceiling and hands the customer to a different product when it is exceeded.**

### In-place major version upgrade

Separately from Blue/Green, RDS has upgraded all in-Region read replicas
concurrently with the primary since 2020. A replica can be skipped by promoting
or deleting it first, but this is not required. Replicas are therefore *not* a
blocker for in-place MVU on AWS.

---

## 3. GCP — no blue/green

Cloud SQL for PostgreSQL offers two documented major-version paths, both with
downtime:

- **In-place**, using `pg_upgrade`, retaining instance name, IP address, and
  settings.
- **Migrate data**, exporting from the current instance and importing into a new
  one, with the source placed in read-only mode for the duration.

The second is a cold export/import, not a replicated cutover. For a live-sync
path, GCP directs customers to Database Migration Service.

**Replicas are not a blocker.** `--include-replicas-for-major-version-upgrade`
upgrades the primary and all replicas, including cross-region and cascading
replicas. Without the flag, replicas must be upgraded *before* the primary.
Neither path requires deleting them. External replicas cannot be upgraded
in place at all.

Two details with direct product relevance:

- **In-place MVU deletes every logical replication slot on the instance.** Slots
  must be recorded beforehand and recreated afterward or replication streams
  break silently.
- **GCP ships a pre-upgrade check.** `pre-check-major-version-upgrade` returns
  findings typed `INFO` / `WARNING` / `ERROR`, each with a message and an
  `actions_required` field; `ERROR` blocks the upgrade. It covers incompatible
  extensions, unsupported dependencies, and data incompatibilities.

GCP also publishes hard numeric ceilings worth borrowing as gate thresholds:
more than 10 million large objects causes the upgrade to fail and roll back;
Cloud SQL is optimised to roughly 100,000 relations, beyond which upgrades are
less reliable and may fail after a six-hour timeout; instances with more than
1,000 databases may time out.

---

## 4. Azure — the widest gap today

No blue/green feature. The migration service is built on `pgcopydb follow` and
inherits its logical-decoding restrictions. It carries a 7-day migration
lifetime and a 72-hour online cutover window, permits only one active migration
per flexible server, and offers no object-level migration. **There is no
first-party Flex-to-Flex path.**

Flex also has a materially larger immutability surface than RDS:

| Property | RDS | Flexible Server |
|---|---|---|
| Public accessibility | Modifiable flag | Creation-time, immutable |
| VPC / VNet | Change the DB subnet group | VNet injection is fixed at creation |
| Cross-region backup | Toggled after creation | Geo-redundant backup is creation-time only |
| Storage type | gp2 / gp3 / io1 / io2 in place | — |
| Storage shrink | Supported via Blue/Green since Nov 2024 | Not possible; tiers double upward only |
| Read replicas at MVU | Upgraded concurrently | Must be deleted first |

Immutable on every platform: encryption state, collation and encoding,
major version downgrade, and cross-account or cross-tenant moves.

---

## 5. Capability classes outside the 1:1 clone model

None of the following is reachable by a staging-clone feature on any cloud,
regardless of how many versions it ships. These are structural, not
roadmap-dependent.

**N:M fleet reshaping**
- Consolidation: several under-utilised servers into one. A live
  cost-optimisation request.
- Split: one server into several, extracting a database or schema onto its own
  instance.
- Flex → Elastic Clusters: a sharding topology change, not a version change.

**Boundaries a clone cannot cross**
- Cross-subscription and cross-tenant moves generally.
- Cross-product: Flexible Server → Azure HorizonDB.
- Exit migrations: to on-premises, another cloud, or self-managed. No vendor
  will build this, and customers ask for it.
- Collation and encoding changes — rebuild-only everywhere.

**Scale beyond a single apply worker**
- Databases whose write rate exceeds what one logical apply process can replay.
  AWS documents this ceiling and redirects to DMS; any Azure implementation
  built on logical replication inherits it. Multi-terabyte, write-heavy estates
  are unserved by blue/green on day one.

**Operations above the single-server unit**
- Fleet-scale orchestration: applying the same change across dozens of servers
  with sequencing, verification, and rollback.
- Rollback after cutover: reverse replication once traffic is on the new server.
  A blue/green deployment leaves a stale former primary, not a path back.

---

## 6. Assessment

The platform features are converging on the same capability and improving
steadily. Any position that depends on vendors being slow is a timing
assumption, not a durable one — and the AWS record argues against it.

The defensible position is orthogonal rather than adjacent: **cross boundaries a
clone cannot cross, handle scale a single apply worker cannot reach, and operate
above the single-server unit.** None of those is on the observed release
trajectory of any of the three clouds.

A second, complementary option is to treat the platform primitive as an
implementation rather than a competitor. Planning, preflight refusal,
convergence estimation, and post-cutover verification are equally useful driving
a vendor's blue/green as driving a self-managed pgcopydb path. With a swappable
loader interface, a future Azure blue/green becomes one more backend — and the
orchestration and judgment layer, which no vendor builds, is retained either
way.

---

## Sources

- RDS Blue/Green overview — https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-overview.html
- RDS Blue/Green limitations and considerations — https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-considerations.html
- PostgreSQL replication methods for blue/green — https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-replication-type.html
- RDS PostgreSQL engine upgrades — https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_UpgradeDBInstance.PostgreSQL.html
- DMS homogeneous data migration limitations — https://docs.aws.amazon.com/dms/latest/userguide/data-migrations.html
- Cloud SQL in-place major version upgrade — https://cloud.google.com/sql/docs/postgres/upgrade-major-db-version-inplace
- Cloud SQL major version upgrade by migrating data — https://cloud.google.com/sql/docs/postgres/upgrade-major-db-version-migrate
- Azure migration service known issues — https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/concepts-known-issues-migration-service
- Azure migration service best practices — https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql
