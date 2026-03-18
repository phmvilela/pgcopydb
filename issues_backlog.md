# Issue 11: Pre-Migration: Storage
If the source instance is running in Azure Cloud, I want you to run a command via Azure CLI or alternative to verify the storage allocated, storage free and storage %usage. If possible, retrieve the amount of storage being used for data and indexes vs backups vs WAL.
The output should be a json file called pg-source-storage.json
---
# Issue 10: Pre-Migration: Networking
If the source instance is running in Azure Cloud, I want you to run a command via Azure CLI or alternative to verify how is the networking configured in the source server. Public Access vs Private Access? As an output, please create a file called pg-source-networking.json which details the networking configuration of the source server.
---
# Issue 9: Pre-Migration: Geo-backup
If the source instance is running in Azure Cloud, I want you to run a command via Azure CLI or alternative to verify if Geo-backup is enabled.
The output should be a file called pg-source-geobackup.json and it contains whether a 0 or 1. If 1, it details the Geo-backup configuration such as paired region.
---
# Issue 8: Pre-Migration: High Availability
If the source instance is running in Azure Cloud, I want you to run a command via Azure CLI or alternative to verify if High Availability is enabled.
The output should be a file called pg-source-ha.json and it contains whether a 0 or 1. If 1, it details the HA configuration such as region, zone redundant or same zone.
---
# Issue 7: Pre-migration: extensions
I want you to connect to the PostgreSQL source database and list all the enabled extensions on the source. The output is called pg-source-extensions.json and it contains a list of the enabled extensions.
---
# Issue 6: Pre-Migration: Migration role
I want you to connect to the PostgreSQL source server and check whether there is a role with pg_azure_admin and with replication privileges.
The output should be called pg-source-migration-role.json and it has success or failure.
---
# Issue 5: Pre-Migration: PK
I want you to connect to the source database and verify that every table in the database has a Primary Key, or a Unique Key, or has replica identity full.
the output json file is called pg-source-pk and contains either a success or a failure. In case of failure, it prints the name of the tables that are lacking this pre-requisite.
---
# Issue 4: Pre-Migration : Parameters
I want you to connect to the source database and create an output json file called pg-source-parameters, which contains all the instance level parameters of the source instance.
---
# Issue 3: Pre-Migration: Metrics
I want you to connect to the source PostgreSQL database and collect metrics that will be useful to the migration pipeline later on.
A few things that comes to mind are: PostgreSQL source version, number of objects (triggers, functions, procedures, tables), partitioning, database size, tables size, etc.

This will be outputted to a json file called source-pg-metrics 
---
# Issue 2: Pre-Migration Stage - Provision a VM in Azure
I need you to provision a Virtual Machine in Azure that runs Ubuntu latest stable version.
this VM should be able run your python application and be able to connect to PostgreSQL.
You should prompt the user on what version of PostgreSQL to install in the VM.

The output of your work here is a terraform template that anybody can run against azure and provision it.

The VM should be created with minimal number of cores & RAM at this point. We will scale the VM later on when needed.
---
# Issue 1: Create a Python app
Create a Python application whose main purpose is to support a side by side migration of a given PostgreSQL environment, to a new PostgreSQL environment in Azure.

The application will have 3 key stages: Pre-migration, Migration & Post-Migration.

There will be a mix of python and terraform templates as an output, given the fact that there will be need for resource provisioning during the migration and post-migration tasks.

---
