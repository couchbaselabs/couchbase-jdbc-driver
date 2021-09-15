# Couchbase JDBC Driver

This project contains the source code for the Couchbase JDBC Driver which supports Analytics and Query (both with different capabilities).

Right now it is a **work in progress** and can only be obtained through the source code. Also, no official enterprise-level support is provided, but feel free to raise a ticket and we'll try to help out as best as we can.

## Usage: Analytics

 - JDBC URL Prefix: `jdbc:couchbase:analytics`
 - A JDBC `Catalog` corresponds to an analytics `Dataverse` or a Couchbase `Bucket` if mapped from collections.
 - A JDBC `Schema` corresponds to a Couchbase `Scope` and is optional.

So if the `travel-sample` bucket is mapped with the `inventory` scope, the catalog is `travel-sample` and the schema is `inventory`.

## Usage: Query

 - JDBC URL Prefix: `jdbc:couchbase:query`

Query support is not enabled right now and will be added at a later point.