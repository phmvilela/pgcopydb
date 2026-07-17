# **PostgreSQL Logical Replication Limitations & Workarounds for LOBs**

This document outlines the technical takeaways from a discussion regarding database replication challenges in PostgreSQL, specifically focusing on how large data types affect synchronization performance and the inherent limitations of logical replication (CDC).

## **1\. Impact of Database Size and Data Types**

When designing a replication or data migration pipeline, the overall size of the database is a primary factor determining how the process is parallelized and how long it will take. However, the specific **data types** present in the schema significantly influence the complexity and strategy required.

* **High-Impact Data Types:** Large Objects (LOBs), images, and large binary objects stored directly within the database drastically increase replication overhead.  
* **Architectural Best Practice:** It is highly recommended to isolate the replication pipeline for these large data types, separating them from standard relational data.

## **2\. The Logical Replication Constraint in PostgreSQL**

PostgreSQL supports several data types for large or structured content, including BLOB/LOB, bytea, XML, and JSON. A critical limitation exists within PostgreSQL's logical replication framework (Change Data Capture / CDC):

* **Lack of Native CDC Support for LOBs:** Logical replication, which relies on interpreting the Write-Ahead Log (WAL) to maintain data deltas, **does not replicate Large Objects (LOBs)** natively. This limitation has been present in PostgreSQL since version 10\.  
* **Tool Differences:** While physical backup tools like pg\_dump can capture LOBs successfully, native logical replication stream mechanisms fail to process them.

## **3\. Proven Workarounds and Migration Strategies**

Since native real-time logical replication is not viable for LOBs, several strategies can be implemented to achieve synchronization:

### **A. Asynchronous Batch Copying**

Instead of attempting real-time streaming, establish a separate batch pipeline. This involves negotiating business SLAs to allow for asynchronous, non-real-time synchronization rather than strict real-time parity.

### **B. Trigger-Based Tracking Tables**

Because actions executed natively on LOBs may not populate the WAL in a way that CDC can parse, a database trigger can be utilized to log changes manually.

* **Mechanism:** A trigger is created on the source table to capture the ID or a timestamp whenever a row containing a large object is inserted or updated.  
* **WAL Utilization:** The trigger writes these identifiers into a standard metadata/tracking table. Because this tracking table uses standard data types, its changes *are* recorded in the WAL and successfully replicated to the target database.  
* **Consumption:** The consumer on the destination side reads the replicated tracking table and fetches the actual large objects in batches directly from the source.

### **C. Timestamping and Schema Modification**

For scenarios that are strictly *insert-only*, adding a timestamp column with a default value of NOW() is sufficient to identify new rows for batch pulling. However, if the data is subject to updates, a trigger-based tracking approach remains mandatory.  
Following standard development best practices—such as explicitly listing target columns during INSERT queries—ensures that adding a tracking or timestamp column to an existing schema will not break downstream application logic.