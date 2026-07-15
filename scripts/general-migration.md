# Zero-Downtime Migration for Architectural Changes: The Side-by-Side Approach

Modifying the fundamental architecture of your database—such as downsizing hardware, changing networking topology, or migrating to a new storage technology—often requires database recreation, which can lead to extended service outages.

This guide details a structured, side-by-side migration path to drastically reduce downtime for these critical architectural shifts. This method is crucial when limitations prevent online, in-place modifications, forcing you to provision a new environment to achieve goals such as:

- Networking Changes: Altering a complex network landscape, like migrating from VNet-injected to Public Access, or changing the VNet itself.

- Storage/Platform Switch: Adopting a new platform, region, or underlying storage technology that requires a fresh instance build.

- Moving to a new tenant

![Side by Side Migration](Side-by-Side-Migration-general-use-case.jpg)

## ⚙️ I. Setup and Provisioning

### Step 0 - Provision the New Target Server

Create a new Azure Database for PostgreSQL instance with your desired new landscape/technology. Ensure the new server's configuration (SKU, storage size, and location) is suitable for your eventual production load.

>Key Concept: This action enables the core benefit of a side-by-side migration: running two distinct environments concurrently. The existing application remains connected to the source environment, minimizing risk and allowing the new target to be fully configured offline.

### Step 1 - Role Privileges (Source & Target servers)
The user role designated for the migration must have the necessary permissions to manage replication and slots.

```sql
ALTER ROLE demo WITH REPLICATION;
GRANT azure_pg_admin TO demo;
```
> The user role designated for the migration must have the necessary permissions to manage replication and slots. The "demo" user was created at server creation. In case you want to create a dedicated replication user, find my guide [here](https://github.com/berenguel/bi-directional-replication-in-Flexible-Server/blob/main/configuring_replication_user.sql)


### Step 2 - Check Prerequisites for Logical Replication (Source & Target servers)
Set these server parameters to at least the minimum recommended values shown below to enable and support the features required for logical replication.

```sql
wal_level=logical
max_worker_processes=16
max_replication_slots=10
max_wal_senders=10
track_commit_timestamp=on
```

### Step 3 - Ensure tables are ready for logical replication

For PostgreSQL to accurately track row-level changes (updates, deletes), every table you plan to replicate must have a unique identifier.

Tables *must have* one of the following:
- a Primary Key, or
- a unique index, or
- Replica identity full (less efficient)

## ➡️ II. Migration and Synchronization

This section details the core process of moving data and establishing synchronization between the two environments.

### Step 4 - Set Up Logical Replication (Source Server)

A Publication defines the logical grouping of tables on the source server that will be replicated

```sql
create publication logical_mig01;
alter publication logical_mig01 add table dummy_test;

SELECT pg_create_logical_replication_slot('logical_mig01', 'pgoutput');
```
> The Safety Net: The slot created in step 3 immediately begins tracking all changes in the WAL. This guarantees that all transactions occurring from this moment forward are recorded and preserved, preventing data loss during the initial dump/restore process

### Step 5 - Generate Schema + Initial Data Dump (Recommended: Azure VM)

Perform the dump after creating the replication slot to capture a static starting point. Using an Azure VM is recommended for optimal network performance.

```
pg_dump -U demo -W \
-h pgprimary6666.postgres.database.azure.com -p 5432 -Fc -v \
-f dump.bak postgres \
	-N pg_catalog \
	-N cron \
	-N information_schema
```


### Step 6 - Restore the data dump into the Target (recommended: Azure VM)

This populates the target server with the initial dataset.
```
pg_restore -U demo -W  \
-h pgprimary6666-1.postgres.database.azure.com -p 5432 --no-owner \
-Fc -v -d postgres dump.bak --no-acl
```

> Catch-Up Mechanism: While the restoration is ongoing, new transactions on the source are safely recorded by the replication slot. It is critical to have sufficient storage on the source to hold the WAL files during this initial period until replication is fully active.

### Step 7 - Create the target server as subscriber & Advance Replication Origin

This step connects the target (subscriber) to the source and manually tells the target where in the WAL log stream to begin reading changes, skipping the data already restored.

```sql
-- On the target
CREATE SUBSCRIPTION logical_sub01 CONNECTION 'host=pgprimary6666.postgres.database.azure.com port=5432 dbname=postgres user=yyyy password=zzzzzzz' PUBLICATION logical_mig01
WITH (
	copy_data = false,
	create_slot = false,
	enabled = false,
	slot_name = 'logical_mig01'
);

SELECT roident, roname FROM pg_replication_origin;
-- Example output:
--  roident |  roname
-- ---------+----------
--        1 | pg_25910

-- On the source:
SELECT slot_name, restart_lsn FROM pg_replication_slots WHERE slot_name = 'logical_mig01';
-- Example output:
--  slot_name    | restart_lsn
-- -------------+-------------
--  logical_mig01 | 20/9300A690

-- On the target (replace 'pg_25910' and '20/9300A690' with your actual values):
SELECT pg_replication_origin_advance('pg_25910', '20/9300A690');
```

> Manual Synchronization: By advancing the origin, you instruct the subscriber to ignore all WAL records before the specified restart_lsn, effectively aligning the subscription with the exact point in time when the initial dump was taken.

Step 8 - Enable the target server as a subscriber of the source server

With the target server populated and the replication origin advanced, you can start the synchronization.

```sql
ALTER SUBSCRIPTION logical_sub01 ENABLE;
```
> Result: The target server now starts consuming the WAL entries from the source, rapidly closing the gap on all transactions that occurred during the dump and restore process.

## ✅ III. Post-Migration & Cutover

Step 9 - Test Replication works
Confirm that the synchronization is working by inserting a record on the source and immediately verifying its presence on the target.
```
--@Source Server
insert into dummy_test values (1, 'London', 'Test', 66, 77, now());
```
```
--@Target Server
select * from dummy_test;
```

Step 10 - The Cutover
Once both databases are synchronized (replication lag stabilizes near zero), you are ready for the minimal downtime cutover.

- 10.1 Stop application traffic to the source database.

- 10.2 Wait for the target database to confirm zero replication lag.

- 10.3 Disable the subscription (ALTER SUBSCRIPTION logical_sub01 DISABLE;).

- 10.4 Connect the application to the new Azure Database for PostgreSQL instance.
	> Recommendation: Utilize Virtual Endpoints or a CNAME DNS record for your database connection string. By simply pointing the endpoint/CNAME to the new server, you can switch your application stack without changing hundreds of individual configuration files, making the final cutover near-instantaneous.
 
