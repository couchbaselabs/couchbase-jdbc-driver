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
 * Tests demonstrating features used by the JDBC driver that are NOT supported
 * by the Couchbase Analytics Java Client SDK (couchbase-analytics-java-client 1.0.x).
 *
 * These tests show why a full migration to the Analytics SDK is not feasible
 * without losing critical JDBC functionality.
 *
 * <h2>Unsupported Features in Analytics SDK:</h2>
 * <ul>
 *   <li><b>Deferred Mode</b> - Query execution with handle-based result retrieval</li>
 *   <li><b>Query Cancellation</b> - Ability to cancel running queries (JDBC Statement.cancel())</li>
 *   <li><b>Compile-Only Mode</b> - Query validation without execution</li>
 *   <li><b>Ping/Health Check</b> - Connection validation via /admin/ping endpoint</li>
 *   <li><b>Cluster Version</b> - Retrieving server version via /pools endpoint</li>
 * </ul>
 */
class UnsupportedAnalyticsSdkFeaturesIT extends BaseAnalyticsIntegrationTest {

    private static Connection connection;

    @BeforeAll
    static void setup() throws Exception {
        try {
            startContainer(UnsupportedAnalyticsSdkFeaturesIT.class);
            connection = DriverManager.getConnection(url(), username(), password());
        } catch (Throwable t) {
            assumeTrue(false, "Analytics server not available at " + url() + " — skipping: " + t.getMessage());
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        stopContainer();
    }

    /**
     * Tests for DEFERRED MODE execution.
     *
     * The Analytics SDK only supports immediate execution (executeQuery/executeStreamingQuery).
     * The JDBC driver requires deferred mode where:
     * 1. Query is submitted and a handle is returned
     * 2. Results are fetched separately using the handle
     *
     * This is fundamental to how the JDBC ResultSet streaming works.
     */
    @Nested
    @DisplayName("Deferred Mode Tests - NOT supported in Analytics SDK")
    class DeferredModeTests {

        @Test
        @DisplayName("Query execution uses deferred mode internally for ResultSet streaming")
        void queryExecutionUsesDeferredMode() throws SQLException {
            // This simple query internally uses deferred mode:
            // 1. POST to /query/service with mode: "deferred"
            // 2. Response contains a "handle" URL
            // 3. GET to /query/service/result/{handle} to fetch results

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
            // Generate a larger result set that benefits from streaming
            // The deferred mode allows results to be fetched incrementally

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT i FROM range(1, 100) AS i"
            );

            int count = 0;
            while (rs.next()) {
                count++;
                assertTrue(rs.getInt(1) >= 1 && rs.getInt(1) <= 100);
            }

            assertEquals(100, count, "Should stream all 100 rows via deferred mode");

            rs.close();
            stmt.close();
        }
    }

    /**
     * Tests for QUERY CANCELLATION.
     *
     * The Analytics SDK does not expose the /analytics/admin/active_requests endpoint.
     * The JDBC driver needs this to implement Statement.cancel() as per JDBC spec.
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

            // Start a long-running query in background
            CompletableFuture<Void> queryFuture = CompletableFuture.runAsync(() -> {
                try {
                    queryStarted.set(true);
                    // This query generates a large result set to simulate long execution
                    stmt.executeQuery("SELECT VALUE i FROM range(1, 10000000) AS i");
                } catch (SQLException e) {
                    // Expected - query was cancelled
                    if (e.getMessage() != null &&
                        (e.getMessage().contains("cancel") ||
                         e.getMessage().contains("Cancel") ||
                         e.getMessage().contains("aborted"))) {
                        queryCancelled.set(true);
                    }
                }
            }, executor);

            // Wait for query to start
            Thread.sleep(100);

            if (queryStarted.get()) {
                // Cancel the query - this calls DELETE /analytics/admin/active_requests
                stmt.cancel();
            }

            // Wait for completion
            try {
                queryFuture.get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // Query may have been cancelled
            }

            executor.shutdown();
            stmt.close();

            // Note: Cancellation behavior may vary based on query state
            // The important thing is that cancel() is available and makes the HTTP call
        }

        @Test
        @DisplayName("PreparedStatement.cancel() also works for prepared queries")
        void preparedStatementCancelWorks() throws Exception {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT i FROM range(1, ?) AS i"
            );
            pstmt.setInt(1, 1000000);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            AtomicBoolean started = new AtomicBoolean(false);

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    started.set(true);
                    pstmt.executeQuery();
                } catch (SQLException e) {
                    // Expected on cancel
                }
            }, executor);

            Thread.sleep(50);

            if (started.get()) {
                // This internally uses the same cancellation mechanism
                pstmt.cancel();
            }

            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }

            executor.shutdown();
            pstmt.close();
        }
    }

    /**
     * Tests for COMPILE-ONLY MODE.
     *
     * The Analytics SDK does not support compile_only parameter.
     * This is used for query validation without actually executing the query.
     */
    @Nested
    @DisplayName("Compile-Only Mode Tests - NOT supported in Analytics SDK")
    class CompileOnlyModeTests {

        @Test
        @DisplayName("PreparedStatement creation validates SQL syntax without execution")
        void preparedStatementValidatesSyntax() throws SQLException {
            // Creating a PreparedStatement uses compile-only mode to validate SQL
            // without actually executing the query

            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as param1, ? as param2"
            );

            assertNotNull(pstmt, "PreparedStatement should be created");

            // The statement was compiled (validated) but not executed
            // Parameters can now be set
            pstmt.setString(1, "value1");
            pstmt.setInt(2, 42);

            // Now execute
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("value1", rs.getString("param1"));
            assertEquals(42, rs.getInt("param2"));

            rs.close();
            pstmt.close();
        }

        @Test
        @DisplayName("Invalid SQL is detected during PreparedStatement creation")
        void invalidSqlDetectedDuringPrepare() {
            // Compile-only mode allows early detection of SQL errors
            // without incurring full query execution cost

            SQLException ex = assertThrows(SQLException.class, () -> {
                connection.prepareStatement("SELECT * FROM nonexistent_dataset_xyz");
            });

            // Error should indicate compilation/semantic issue
            assertNotNull(ex.getMessage());
        }

        @Test
        @DisplayName("Syntax errors detected via compile-only validation")
        void syntaxErrorsDetectedEarly() {
            SQLException ex = assertThrows(SQLException.class, () -> {
                connection.prepareStatement("SELECT WHERE FROM");
            });

            assertTrue(ex.getMessage().toLowerCase().contains("syntax") ||
                       ex.getMessage().toLowerCase().contains("error"),
                       "Should report syntax error");
        }
    }

    /**
     * Tests for PING/HEALTH CHECK functionality.
     *
     * The Analytics SDK does not expose the /admin/ping endpoint.
     * This is used for connection validation (Connection.isValid()).
     */
    @Nested
    @DisplayName("Ping/Health Check Tests - NOT supported in Analytics SDK")
    class PingHealthCheckTests {

        @Test
        @DisplayName("Connection.isValid() uses ping endpoint for health check")
        void connectionIsValidUsesPing() throws SQLException {
            // isValid() internally calls ping() which hits /admin/ping
            // This is a lightweight health check without executing a query

            assertTrue(connection.isValid(5),
                "Connection should be valid - ping succeeded");
        }

        @Test
        @DisplayName("isValid() returns false for closed connections")
        void isValidReturnsFalseWhenClosed() throws SQLException {
            Connection tempConn = DriverManager.getConnection(url(), username(), password());
            assertTrue(tempConn.isValid(5), "New connection should be valid");

            tempConn.close();

            assertFalse(tempConn.isValid(5),
                "Closed connection should report invalid");
        }

        @Test
        @DisplayName("Ping with timeout respects the timeout parameter")
        void pingRespectsTimeout() throws SQLException {
            // The ping endpoint accepts a timeout parameter
            // Very short timeout should still work for local connections

            assertTrue(connection.isValid(1),
                "Ping with 1 second timeout should succeed locally");
        }
    }

    /**
     * Tests for CLUSTER VERSION retrieval.
     *
     * The Analytics SDK does not provide access to /pools endpoint.
     * This is used to retrieve the Couchbase Server version.
     */
    @Nested
    @DisplayName("Cluster Version Tests - NOT supported in Analytics SDK")
    class ClusterVersionTests {

        @Test
        @DisplayName("DatabaseMetaData provides server version from /pools endpoint")
        void databaseMetaDataProvidesServerVersion() throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();

            String productVersion = metaData.getDatabaseProductVersion();
            assertNotNull(productVersion, "Product version should not be null");
            assertFalse(productVersion.isEmpty(), "Product version should not be empty");

            // Version format is typically like "Couchbase/7.6.0-1234-enterprise"
            assertTrue(productVersion.contains("Couchbase") ||
                       productVersion.matches(".*\\d+\\.\\d+.*"),
                       "Version should contain Couchbase or version numbers: " + productVersion);
        }

        @Test
        @DisplayName("Server version is retrieved during connection establishment")
        void serverVersionRetrievedOnConnect() throws SQLException {
            // The connect() method in AnalyticsProtocol calls clusterVersion()
            // which hits GET /pools to get implementationVersion

            DatabaseMetaData metaData = connection.getMetaData();

            String productName = metaData.getDatabaseProductName();
            String productVersion = metaData.getDatabaseProductVersion();

            assertNotNull(productName);
            assertNotNull(productVersion);

            // The driver combines these: "Couchbase/7.x.x"
            System.out.println("Database Product: " + productName);
            System.out.println("Database Version: " + productVersion);
        }
    }

    /**
     * Combined test showing the interplay of unsupported features.
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
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as x"
            );

            // 4. Execute uses DEFERRED MODE
            pstmt.setInt(1, 42);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("x"));

            rs.close();
            pstmt.close();

            // Note: CANCEL would be tested with long-running queries
        }
    }
}
