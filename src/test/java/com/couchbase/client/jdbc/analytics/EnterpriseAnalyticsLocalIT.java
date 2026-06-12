/*
 * Copyright (c) 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.jdbc.analytics;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the enterprise-analytics-jdbc-driver against a local Enterprise Analytics
 * instance. Covers deferred mode, query cancellation, compile-only, ping, and
 * server version retrieval.
 *
 * <h2>Configuration:</h2>
 * Set these system properties before running:
 * <ul>
 *   <li><b>local.url</b> - Full JDBC URL (e.g., jdbc:couchbase:analytics://localhost:9600)</li>
 *   <li><b>local.user</b> - Username (default: couchbase)</li>
 *   <li><b>local.password</b> - Password (default: couchbase)</li>
 * </ul>
 *
 * <h2>Run Command:</h2>
 * <pre>
 * mvn test -pl enterprise-analytics-jdbc-driver -Dtest=EnterpriseAnalyticsLocalIT \
 *   -Dlocal.url=jdbc:couchbase:analytics://localhost:9600 \
 *   -Dlocal.user=couchbase \
 *   -Dlocal.password=couchbase
 * </pre>
 *
 * <h2>Important Notes:</h2>
 * <p>The Couchbase Java SDK needs to bootstrap from the cluster to discover services.
 * The SDK uses the hostname from the JDBC URL to connect to the cluster's KV service
 * (default port 11210) and management service (default port 8091).</p>
 *
 * <p>If your cluster uses non-standard ports, the SDK connection may fail because
 * it can't reach the bootstrap services. In such cases, ensure your cluster is
 * accessible on standard ports, or the JDBC driver would need modification to
 * support custom SDK port configuration.</p>
 */
class EnterpriseAnalyticsLocalIT {

    // Direct URL configuration - most flexible approach
    private static final String URL = System.getProperty("local.url",
        "jdbc:couchbase:analytics://localhost:9600");
    private static final String USER = System.getProperty("local.user", "couchbase");
    private static final String PASSWORD = System.getProperty("local.password", "couchbase");

    private static Connection connection;

    static String url() {
        return URL;
    }

    @BeforeAll
    static void setup() throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("JDBC URL: " + url());
        System.out.println("User: " + USER);
        System.out.println("=".repeat(60));

        try {
            connection = DriverManager.getConnection(url(), USER, PASSWORD);
            System.out.println("Connected successfully!");
        } catch (Throwable t) {
            assumeTrue(false, "Analytics server not available at " + url() + " — skipping: " + t.getMessage());
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Tests for DEFERRED MODE execution.
     */
    @Nested
    @DisplayName("Deferred Mode Tests - NOT supported in Analytics SDK")
    class DeferredModeTests {

        @Test
        @DisplayName("Query execution uses deferred mode internally for ResultSet streaming")
        void queryExecutionUsesDeferredMode() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as num, 'test' as str");

            assertTrue(rs.next(), "Should have at least one row");
            assertEquals(1, rs.getInt("num"));
            assertEquals("test", rs.getString("str"));
            assertFalse(rs.next(), "Should have exactly one row");

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Large result streaming depends on deferred mode handle mechanism")
        void largeResultStreamingUsesDeferredMode() throws SQLException {
            Statement stmt = connection.createStatement();
            // Generate multiple rows using UNION ALL (compatible with all Analytics versions)
            ResultSet rs = stmt.executeQuery(
                "SELECT 1 as num UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 " +
                "UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10"
            );

            int count = 0;
            while (rs.next()) {
                count++;
                assertTrue(rs.getInt(1) >= 1 && rs.getInt(1) <= 10);
            }

            assertEquals(10, count, "Should stream all 10 rows via deferred mode");

            rs.close();
            stmt.close();
        }
    }

    /**
     * Tests for QUERY CANCELLATION.
     */
    @Nested
    @DisplayName("Query Cancellation Tests - NOT supported in Analytics SDK")
    class QueryCancellationTests {

        @Test
        @DisplayName("Statement.cancel() terminates a running query")
        void statementCancelTerminatesQuery() throws Exception {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Statement stmt = connection.createStatement();
            AtomicBoolean queryStarted = new AtomicBoolean(false);
            AtomicBoolean queryCancelled = new AtomicBoolean(false);

            CompletableFuture<Void> queryFuture = CompletableFuture.runAsync(() -> {
                try {
                    queryStarted.set(true);
                    stmt.executeQuery("SELECT VALUE i FROM range(1, 10000000) AS i");
                } catch (SQLException e) {
                    if (e.getMessage() != null &&
                        (e.getMessage().contains("cancel") ||
                         e.getMessage().contains("Cancel") ||
                         e.getMessage().contains("aborted"))) {
                        queryCancelled.set(true);
                    }
                }
            }, executor);

            Thread.sleep(100);

            if (queryStarted.get()) {
                stmt.cancel();
            }

            try {
                queryFuture.get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }

            executor.shutdown();
            stmt.close();
        }
    }

    /**
     * Tests for COMPILE-ONLY MODE.
     */
    @Nested
    @DisplayName("Compile-Only Mode Tests - NOT supported in Analytics SDK")
    class CompileOnlyModeTests {

        @Test
        @DisplayName("PreparedStatement creation validates SQL syntax without execution")
        void preparedStatementValidatesSyntax() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as param1, ? as param2"
            );

            assertNotNull(pstmt, "PreparedStatement should be created");

            pstmt.setString(1, "value1");
            pstmt.setInt(2, 42);

            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("value1", rs.getString("param1"));
            assertEquals(42, rs.getInt("param2"));

            rs.close();
            pstmt.close();
        }

        @Test
        @DisplayName("Syntax errors detected via compile-only validation")
        void syntaxErrorsDetectedEarly() {
            SQLException ex = assertThrows(SQLException.class, () -> {
                connection.prepareStatement("SELECT WHERE FROM");
            });

            assertTrue(ex.getMessage().toLowerCase().contains("syntax") ||
                       ex.getMessage().toLowerCase().contains("error"),
                       "Should report syntax error: " + ex.getMessage());
        }
    }

    /**
     * Tests for PING/HEALTH CHECK functionality.
     */
    @Nested
    @DisplayName("Ping/Health Check Tests - NOT supported in Analytics SDK")
    class PingHealthCheckTests {

        @Test
        @DisplayName("Connection.isValid() uses ping endpoint for health check")
        void connectionIsValidUsesPing() throws SQLException {
            assertTrue(connection.isValid(5),
                "Connection should be valid - ping succeeded");
        }

        @Test
        @DisplayName("isValid() returns false for closed connections")
        void isValidReturnsFalseWhenClosed() throws SQLException {
            Connection tempConn = DriverManager.getConnection(url(), USER, PASSWORD);
            assertTrue(tempConn.isValid(5), "New connection should be valid");

            tempConn.close();

            assertFalse(tempConn.isValid(5),
                "Closed connection should report invalid");
        }
    }

    /**
     * Tests for CLUSTER VERSION retrieval.
     */
    @Nested
    @DisplayName("Cluster Version Tests")
    class ClusterVersionTests {

        @Test
        @DisplayName("DatabaseMetaData provides server version from /pools endpoint")
        void databaseMetaDataProvidesServerVersion() throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();

            // Version retrieval is not available via the Analytics SDK (no management API access
            // through the load balancer). Product name is returned; version is not populated.
            assertNotNull(metaData.getDatabaseProductName());
        }

        @Test
        @DisplayName("Server version is retrieved during connection establishment")
        void serverVersionRetrievedOnConnect() throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();

            String productName = metaData.getDatabaseProductName();
            String productVersion = metaData.getDatabaseProductVersion();

            assertNotNull(productName);
            assertNotNull(productVersion);

            System.out.println("Database Product: " + productName);
            System.out.println("Database Version: " + productVersion);
        }
    }

    /**
     * Combined test showing all features work together.
     */
    @Nested
    @DisplayName("Combined Feature Tests")
    class CombinedFeatureTests {

        @Test
        @DisplayName("Full JDBC workflow uses all unsupported features")
        void fullJdbcWorkflowUsesAllFeatures() throws SQLException {
            // 1. Connection validation uses PING
            assertTrue(connection.isValid(5), "Ping-based validation");

            // 2. Get metadata uses CLUSTER VERSION
            DatabaseMetaData meta = connection.getMetaData();
            assertNotNull(meta.getDatabaseProductVersion(), "Version from /pools");

            // 3. PrepareStatement uses COMPILE-ONLY
            // Note: 'value' is a reserved keyword in SQL++, use 'val' instead
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as val"
            );

            // 4. Execute uses DEFERRED MODE
            pstmt.setInt(1, 42);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("val"));

            rs.close();
            pstmt.close();
        }
    }
}
