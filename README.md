# Couchbase JDBC Driver

[![license](https://img.shields.io/github/license/couchbase/couchbase-jvm-clients?color=brightgreen)](https://opensource.org/licenses/Apache-2.0)

This project contains the source code for the Couchbase JDBC Driver which supports Enterprise Analytics and Couchbase Server Analytics. Its main purpose is to provide connectivity to any BI tool that supports a generic JDBC connection. Some examples of BI tools that support generic JDBC connectivity and should work include tools like: Qlik (Qlik Sense), SAP BusinessObjects, IBM Cognos, MicroStrategy, Looker, ThoughtSpot, DBeaver, and others.

## Prerequisites
- Java environment JDK 8+.
- The Couchbase JDBC Driver is compatible with both Enterprise Analytics and Couchbase Server Analytics (note: it does not support the Query Service).
- **Analytics Tabular Views:** Pre-configured [tabular views](https://docs.couchbase.com/analytics/sqlpp/5a_views.html) for data access

## Build Flavors

Two driver flavors are published, each targeting a different Couchbase deployment:

| Artifact | Target | Protocol |
|---|---|---|
| `enterprise-analytics-jdbc-driver` | Enterprise Analytics | Analytics Java SDK |
| `couchbase-analytics-jdbc-driver` | Couchbase Server Analytics | Java SDK (core) |

## Setup/Installation

### Enterprise Analytics — `enterprise-analytics-jdbc-driver`

#### Maven
```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>enterprise-analytics-jdbc-driver</artifactId>
    <version>2.0.0</version>
</dependency>
```

#### Gradle
```groovy
implementation 'com.couchbase.client:enterprise-analytics-jdbc-driver:2.0.0'
```

### Couchbase Server Analytics — `couchbase-analytics-jdbc-driver`

#### Maven
```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>couchbase-analytics-jdbc-driver</artifactId>
    <version>1.2.0</version>
</dependency>
```

#### Gradle
```groovy
implementation 'com.couchbase.client:couchbase-analytics-jdbc-driver:1.2.0'
```

### Jar File
Download the latest version from Maven Central and add it to your classpath.

## Configuration

- **Driver Class Name:** `com.couchbase.client.jdbc.CouchbaseDriver`
- **JDBC URL prefix:** `jdbc:couchbase:analytics` or `jdbc:cb:analytics` as a fallback.
- **Connection String Format:**
```
jdbc:couchbase:analytics//[hostname]/<databaseName>/<scopeName>[?property1=value1[&property2=value2]...]
```

## Examples

### Basic Connection

```java
Connection conn = DriverManager.getConnection(
    "jdbc:couchbase:analytics://127.0.0.1/foo/bar", 
    "user1", 
    "test_password"
);

// Using Properties
Properties props = new Properties();
props.put("user", "user1");
props.put("password", "test_password");
props.put("ssl", "true");
props.put("sslCertPath", "/path/to/cert.pem");
props.put("connectTimeout", "10s");
Connection conn = DriverManager.getConnection(
        "jdbc:couchbase:analytics://jdbc:couchbase:analytics://127.0.0.1/foo/bar", 
        props
);
```

### Query Execution

```java
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM `my_view` LIMIT 10");
while (rs.next()) {
    System.out.println(rs.getString("column_name"));
}
rs.close();
stmt.close();
conn.close();
```

## Connection Properties

Property | Required | Default | Valid Values | Description
--- | --- | --- | --- | ---
`user` | Yes | None | Any string | Username to connect to the database as
`password` | Yes | None | Any string | Password to use when authenticating
`connectTimeout` | No | None | Duration format (e.g., `"10s"`) | Time the driver waits during bootstrap to establish connections before giving up. If not set, lazy bootstrap continues in background (default op timeout is 75s). Format: `"10s"`, `"500ms"`, etc.
`catalogDataverseMode` | No | `"catalog"` | `"catalog"`, `"catalogSchema"` | Defines how the catalog should be interpreted. `"catalog"` uses bucket and scope for the catalog; `"catalogSchema"` uses bucket as catalog and scope as schema.
`catalogIncludesSchemaless` | No | `"false"` | `"true"`, `"false"` | If the Catalog API should also include schemaless catalogs.
`maxWarnings` | No | `"10"` | Any integer | The maximum number of warnings that should be emitted.
`sqlCompatMode` | No | `"true"` | `"true"`, `"false"` | If the analytics SQL compatibility mode should be used.
`scanConsistency` | No | None | `"notBounded"`, `"requestPlus"` | The scanConsistency to use for a query.
`scanWait` | No | None | Any string | The scanWait value to use for a query.
`ssl` | No | `"false"` | `"true"`, `"false"` | Set to true if transport encryption (TLS) should be enabled.
`sslMode` | No | `"verify-full"` | `"verify-full"`, `"verify-ca"`, `"no-verify"` | Defines certificate/hostname verification: `"verify-full"` checks cert + hostname, `"verify-ca"` checks cert only, `"no-verify"` skips all checks (**insecure**).
`sslCertPath` | No | None | File path | The absolute path to the TLS certificate.
`sslKeystorePath` | No | None | File path | The absolute path to the Java keystore.
`sslKeystorePassword` | No | None | Any string | The password for the keystore.
`minDriverVersion` | No | None | Version string | Minimally required driver version.
`minDatabaseVersion` | No | None | Version string | Minimally required database version.

## Limitations
- **Read-Only Operations:** Only SELECT queries are supported
- **Tabular Views Required:** Data must be accessed through pre-configured tabular views
- **No DML/DDL Operations:** INSERT, UPDATE, DELETE, and DDL statements are not supported
