# Couchbase JDBC Driver

[![license](https://img.shields.io/github/license/couchbase/couchbase-jvm-clients?color=brightgreen)](https://opensource.org/licenses/Apache-2.0)

This project contains the source code for the Couchbase JDBC Driver which supports the Analytics Service (not Query!). Its main purpose is to provide the low-level glue to facilitate integration with high level BI-Tools like Tableau.

## Prerequisites
- Java environment JDK 8+.
- The Couchbase JDBC Driver is compatible with Couchbase Enterprise Analytics and the Analytics Service (note: it does not support the Query Service). Its main purpose is to provide the low-level glue needed to facilitate integration with business intelligence (BI) tools.
- **Analytics Tabular Views:** Pre-configured [tabular views](https://docs.couchbase.com/analytics/sqlpp/5a_views.html) for data access

## Setup/Installation

### Maven
Add the following dependency to your `pom.xml`
```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>couchbase-jdbc-driver</artifactId>
    <version>1.0.5</version>
</dependency>
```

### Gradle
Add the following dependency to your `build.gradle`
```groovy
implementation 'com.couchbase.client:couchbase-jdbc-driver:1.0.5'
```

### Jar File
Download the latest version of the JDBC driver from [Maven Central](https://repo1.maven.org/maven2/com/couchbase/client/couchbase-jdbc-driver/) and add it to your classpath.

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
- **Analytics Service Only:** Query Service is not supported
- **Tabular Views Required:** Data must be accessed through pre-configured tabular views
- **No DML/DDL Operations:** INSERT, UPDATE, DELETE, and DDL statements are not supported
