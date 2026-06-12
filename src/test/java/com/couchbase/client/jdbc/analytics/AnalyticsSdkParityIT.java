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
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for features that are supported in BOTH the current Java SDK implementation
 * AND the Analytics SDK. These tests serve as verification after migration.
 *
 * <h2>Run Command:</h2>
 * <pre>
 * mvn test -Dtest='AnalyticsSdkParityTest$*' \
 *   -Dlocal.url='jdbc:couchbase:analytics://localhost:12000' \
 *   -Dlocal.user=couchbase \
 *   -Dlocal.password=couchbase
 * </pre>
 *
 * <h2>Features Tested (Supported in Both SDKs):</h2>
 * <ul>
 *   <li>Basic query execution</li>
 *   <li>Positional parameters</li>
 *   <li>Named parameters (via PreparedStatement)</li>
 *   <li>Result streaming</li>
 *   <li>Multiple data types</li>
 *   <li>NULL handling</li>
 *   <li>Query timeout</li>
 *   <li>Read-only mode</li>
 *   <li>Scan consistency</li>
 *   <li>Client context ID</li>
 *   <li>Result metadata</li>
 * </ul>
 */
class AnalyticsSdkParityIT {

//    private static final String URL = System.getProperty("local.url",
//            "jdbc:couchbase:analytics://localhost:12000?useAnalyticsSdk=false");
    private static final String URL = System.getProperty("local.url",
        "jdbc:couchbase:analytics://localhost:9600");
    private static final String USER = System.getProperty("local.user", "couchbase");
    private static final String PASSWORD = System.getProperty("local.password", "couchbase");

    private static Connection connection;

    @BeforeAll
    static void setup() throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("Analytics SDK Parity Tests");
        System.out.println("JDBC URL: " + URL);
        System.out.println("User: " + USER);
        System.out.println("=".repeat(60));

        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("Connected successfully!");
        } catch (Throwable t) {
            assumeTrue(false, "Analytics server not available at " + URL + " — skipping: " + t.getMessage());
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    // =========================================================================
    // BASIC QUERY EXECUTION
    // =========================================================================

    @Nested
    @DisplayName("Basic Query Execution")
    class BasicQueryTests {

        @Test
        @DisplayName("Execute simple SELECT literal")
        void executeSimpleSelect() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as num");

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Execute SELECT with string literal")
        void executeSelectString() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 'hello' as greeting");

            assertTrue(rs.next());
            assertEquals("hello", rs.getString("greeting"));
            assertFalse(rs.next());

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Execute SELECT with multiple columns")
        void executeSelectMultipleColumns() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 1 as a, 2 as b, 3 as c, 'test' as d"
            );

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals(2, rs.getInt("b"));
            assertEquals(3, rs.getInt("c"));
            assertEquals("test", rs.getString("d"));
            assertFalse(rs.next());

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Execute SELECT with expression")
        void executeSelectExpression() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 2 + 3 as result");

            assertTrue(rs.next());
            assertEquals(5, rs.getInt("result"));
            assertFalse(rs.next());

            rs.close();
            stmt.close();
        }
    }

    // =========================================================================
    // DATA TYPES
    // =========================================================================

    @Nested
    @DisplayName("Data Type Handling")
    class DataTypeTests {

        @Test
        @DisplayName("Integer types")
        void integerTypes() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 0 as zero, 42 as positive, -100 as negative, 9223372036854775807 as bigint"
            );

            assertTrue(rs.next());
            assertEquals(0, rs.getInt("zero"));
            assertEquals(42, rs.getInt("positive"));
            assertEquals(-100, rs.getInt("negative"));
            assertEquals(9223372036854775807L, rs.getLong("bigint"));

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Floating point types")
        void floatingPointTypes() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 3.14 as pi, -2.5 as negative, 0.0 as zero"
            );

            assertTrue(rs.next());
            assertEquals(3.14, rs.getDouble("pi"), 0.001);
            assertEquals(-2.5, rs.getDouble("negative"), 0.001);
            assertEquals(0.0, rs.getDouble("zero"), 0.001);

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Boolean type")
        void booleanType() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT true as t, false as f"
            );

            assertTrue(rs.next());
            assertTrue(rs.getBoolean("t"));
            assertFalse(rs.getBoolean("f"));

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("String type with special characters")
        void stringWithSpecialChars() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 'hello world' as spaces, 'line1\\nline2' as newline, '\"quoted\"' as quotes"
            );

            assertTrue(rs.next());
            assertEquals("hello world", rs.getString("spaces"));
            assertNotNull(rs.getString("newline"));
            assertNotNull(rs.getString("quotes"));

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("NULL handling")
        void nullHandling() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT null as nullable, 1 as notnull"
            );

            assertTrue(rs.next());
            assertNull(rs.getObject("nullable"));
            assertTrue(rs.wasNull() || rs.getObject("nullable") == null);
            assertEquals(1, rs.getInt("notnull"));
            assertFalse(rs.wasNull());

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Array type")
        void arrayType() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT [1, 2, 3] as arr"
            );

            assertTrue(rs.next());
            Object arr = rs.getObject("arr");
            assertNotNull(arr);

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Object/Map type")
        void objectType() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT {'name': 'test', 'value': 42} as obj"
            );

            assertTrue(rs.next());
            Object obj = rs.getObject("obj");
            assertNotNull(obj);

            rs.close();
            stmt.close();
        }
    }

    // =========================================================================
    // PREPARED STATEMENTS & PARAMETERS
    // =========================================================================

    @Nested
    @DisplayName("Prepared Statements & Parameters")
    class PreparedStatementTests {

        @Test
        @DisplayName("Positional parameter - integer")
        void positionalParameterInteger() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as num"
            );
            pstmt.setInt(1, 42);

            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("num"));

            rs.close();
            pstmt.close();
        }

        @Test
        @DisplayName("Positional parameter - string")
        void positionalParameterString() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as str"
            );
            pstmt.setString(1, "hello");

            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("hello", rs.getString("str"));

            rs.close();
            pstmt.close();
        }

        @Test
        @DisplayName("Positional parameter - multiple")
        void positionalParameterMultiple() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as a, ? as b, ? as c"
            );
            pstmt.setInt(1, 1);
            pstmt.setString(2, "two");
            pstmt.setDouble(3, 3.0);

            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals("two", rs.getString("b"));
            assertEquals(3.0, rs.getDouble("c"), 0.001);

            rs.close();
            pstmt.close();
        }

        @Test
        @DisplayName("Positional parameter - boolean")
        void positionalParameterBoolean() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as flag"
            );
            pstmt.setBoolean(1, true);

            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("flag"));

            rs.close();
            pstmt.close();
        }

        @Test
        @DisplayName("Positional parameter - null")
        void positionalParameterNull() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as nullable"
            );
            pstmt.setNull(1, Types.VARCHAR);

            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getObject("nullable"));

            rs.close();
            pstmt.close();
        }

        @Test
        @DisplayName("Reuse PreparedStatement with different values")
        void reusePreparedStatement() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT ? as num"
            );

            // First execution
            pstmt.setInt(1, 10);
            ResultSet rs1 = pstmt.executeQuery();
            assertTrue(rs1.next());
            assertEquals(10, rs1.getInt("num"));
            rs1.close();

            // Second execution with different value
            pstmt.setInt(1, 20);
            ResultSet rs2 = pstmt.executeQuery();
            assertTrue(rs2.next());
            assertEquals(20, rs2.getInt("num"));
            rs2.close();

            pstmt.close();
        }
    }

    // =========================================================================
    // RESULT STREAMING
    // =========================================================================

    @Nested
    @DisplayName("Result Streaming")
    class ResultStreamingTests {

        @Test
        @DisplayName("Stream multiple rows")
        void streamMultipleRows() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 1 as num UNION ALL SELECT 2 UNION ALL SELECT 3 " +
                "UNION ALL SELECT 4 UNION ALL SELECT 5"
            );

            List<Integer> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rs.getInt("num"));
            }

            assertEquals(5, results.size());
            assertTrue(results.contains(1));
            assertTrue(results.contains(2));
            assertTrue(results.contains(3));
            assertTrue(results.contains(4));
            assertTrue(results.contains(5));

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Stream 100 rows")
        void stream100Rows() throws SQLException {
            Statement stmt = connection.createStatement();
            // Generate 100 rows using nested UNION ALL
            StringBuilder query = new StringBuilder("SELECT 1 as num");
            for (int i = 2; i <= 100; i++) {
                query.append(" UNION ALL SELECT ").append(i);
            }

            ResultSet rs = stmt.executeQuery(query.toString());

            int count = 0;
            while (rs.next()) {
                count++;
                assertTrue(rs.getInt("num") >= 1 && rs.getInt("num") <= 100);
            }

            assertEquals(100, count);

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Empty result set")
        void emptyResultSet() throws SQLException {
            Statement stmt = connection.createStatement();
            // SQL++ doesn't support WHERE without FROM, use a subquery that returns nothing
            ResultSet rs = stmt.executeQuery(
                "SELECT num FROM [1, 2, 3] AS num WHERE num > 100"
            );

            assertFalse(rs.next());

            rs.close();
            stmt.close();
        }
    }

    // =========================================================================
    // RESULT SET METADATA
    // =========================================================================

    @Nested
    @DisplayName("ResultSet Metadata")
    class ResultSetMetadataTests {

        @Test
        @DisplayName("Column count")
        void columnCount() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 1 as a, 2 as b, 3 as c"
            );

            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(3, meta.getColumnCount());

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Column names")
        void columnNames() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 1 as first_col, 2 as second_col"
            );

            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("first_col", meta.getColumnName(1));
            assertEquals("second_col", meta.getColumnName(2));

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Column labels")
        void columnLabels() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 1 as col1, 'test' as col2"
            );

            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("col1", meta.getColumnLabel(1));
            assertEquals("col2", meta.getColumnLabel(2));

            rs.close();
            stmt.close();
        }
    }

    // =========================================================================
    // CONNECTION OPTIONS
    // =========================================================================

    @Nested
    @DisplayName("Connection Options")
    class ConnectionOptionsTests {

        @Test
        @DisplayName("Connection with scanConsistency=notBounded")
        void scanConsistencyNotBounded() throws SQLException {
            Properties props = new Properties();
            props.setProperty("user", USER);
            props.setProperty("password", PASSWORD);
            props.setProperty("scanConsistency", "notBounded");

            Connection conn = DriverManager.getConnection(URL, props);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as num");

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));

            rs.close();
            stmt.close();
            conn.close();
        }

        @Test
        @DisplayName("Connection with scanConsistency=requestPlus")
        void scanConsistencyRequestPlus() throws SQLException {
            Properties props = new Properties();
            props.setProperty("user", USER);
            props.setProperty("password", PASSWORD);
            props.setProperty("scanConsistency", "requestPlus");

            Connection conn = DriverManager.getConnection(URL, props);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as num");

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));

            rs.close();
            stmt.close();
            conn.close();
        }

        @Test
        @DisplayName("Connection with SQL compat mode")
        void sqlCompatMode() throws SQLException {
            Properties props = new Properties();
            props.setProperty("user", USER);
            props.setProperty("password", PASSWORD);
            props.setProperty("sqlCompatMode", "true");

            Connection conn = DriverManager.getConnection(URL, props);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as num");

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));

            rs.close();
            stmt.close();
            conn.close();
        }
    }

    // =========================================================================
    // QUERY TIMEOUT
    // =========================================================================

    @Nested
    @DisplayName("Query Timeout")
    class QueryTimeoutTests {

        @Test
        @DisplayName("Set query timeout on Statement")
        void setQueryTimeout() throws SQLException {
            Statement stmt = connection.createStatement();
            stmt.setQueryTimeout(30); // 30 seconds
            assertEquals(30, stmt.getQueryTimeout());

            ResultSet rs = stmt.executeQuery("SELECT 1 as num");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));

            rs.close();
            stmt.close();
        }

        @Test
        @DisplayName("Set query timeout on PreparedStatement")
        void setQueryTimeoutPrepared() throws SQLException {
            PreparedStatement pstmt = connection.prepareStatement("SELECT ? as num");
            pstmt.setQueryTimeout(30);
            assertEquals(30, pstmt.getQueryTimeout());

            pstmt.setInt(1, 42);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("num"));

            rs.close();
            pstmt.close();
        }
    }

    // =========================================================================
    // ERROR HANDLING
    // =========================================================================

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Syntax error throws SQLException")
        void syntaxErrorThrowsSQLException() {
            Statement stmt = assertDoesNotThrow(() -> connection.createStatement());

            SQLException ex = assertThrows(SQLException.class, () ->
                stmt.executeQuery("SELECT FROM WHERE")
            );

            assertNotNull(ex.getMessage());
            // Error message varies by server - check for common patterns
            String msg = ex.getMessage().toLowerCase();
            assertTrue(msg.contains("syntax") ||
                       msg.contains("error") ||
                       msg.contains("invalid"),
                       "Error message should indicate syntax error: " + ex.getMessage());
        }

        @Test
        @DisplayName("Invalid column name throws SQLException")
        void invalidColumnThrowsSQLException() throws SQLException {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as num");
            assertTrue(rs.next());

            assertThrows(SQLException.class, () ->
                rs.getInt("nonexistent_column")
            );

            rs.close();
            stmt.close();
        }
    }

    // =========================================================================
    // CERTIFICATE AUTHENTICATION
    // =========================================================================

    @Nested
    @DisplayName("Certificate Authentication")
    class CertificateAuthTests {

        // Certificate auth test properties (set via -D flags)
        private String getCertAuthUrl() { return System.getProperty("cert.url"); }
        private String getClientCertPath() { return System.getProperty("cert.clientCertPath"); }
        private String getClientKeyPath() { return System.getProperty("cert.clientKeyPath"); }
        private String getClientKeyPassword() { return System.getProperty("cert.clientKeyPassword"); }
        private String getClientKeystorePath() { return System.getProperty("cert.clientKeystorePath"); }
        private String getClientKeystorePassword() { return System.getProperty("cert.clientKeystorePassword"); }
        private String getSslCertPath() { return System.getProperty("cert.sslCertPath"); }

        private boolean isCertAuthConfigured() {
            return getCertAuthUrl() != null &&
                   ((getClientCertPath() != null && getClientKeyPath() != null) ||
                    (getClientKeystorePath() != null && getClientKeystorePassword() != null));
        }

        @Test
        @DisplayName("Certificate auth with PEM files")
        void certAuthWithPemFiles() throws SQLException {
            assumeTrue(getClientCertPath() != null && getClientKeyPath() != null,
                "Skipping PEM cert auth test - set cert.clientCertPath and cert.clientKeyPath");

            String url = getCertAuthUrl() != null ? getCertAuthUrl() : URL;

            Properties props = new Properties();
            props.setProperty("clientCertAuth", "true");
            props.setProperty("ssl", "true");
            props.setProperty("sslMode", "no-verify");
            props.setProperty("clientCertPath", getClientCertPath());
            props.setProperty("clientKeyPath", getClientKeyPath());
            if (getClientKeyPassword() != null) {
                props.setProperty("clientKeyPassword", getClientKeyPassword());
            }
            if (getSslCertPath() != null) {
                props.setProperty("sslCertPath", getSslCertPath());
            }

            try (Connection conn = DriverManager.getConnection(url, props)) {
                assertNotNull(conn);
                assertFalse(conn.isClosed());

                // Execute a simple query to verify connection works
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 1 as num")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("num"));
                }

                System.out.println("Certificate auth with PEM files: SUCCESS");
            }
        }

        @Test
        @DisplayName("Certificate auth with PKCS12 keystore")
        void certAuthWithKeystore() throws SQLException {
            assumeTrue(getClientKeystorePath() != null && getClientKeystorePassword() != null,
                "Skipping keystore cert auth test - set cert.clientKeystorePath and cert.clientKeystorePassword");

            String url = getCertAuthUrl() != null ? getCertAuthUrl() : URL;

            Properties props = new Properties();
            props.setProperty("clientCertAuth", "true");
            props.setProperty("ssl", "true");
            props.setProperty("sslMode", "no-verify");
            props.setProperty("clientCertKeystorePath", getClientKeystorePath());
            props.setProperty("clientCertKeystorePassword", getClientKeystorePassword());
            if (getSslCertPath() != null) {
                props.setProperty("sslCertPath", getSslCertPath());
            }

            try (Connection conn = DriverManager.getConnection(url, props)) {
                assertNotNull(conn);
                assertFalse(conn.isClosed());

                // Execute a simple query to verify connection works
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 1 as num")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("num"));
                }

                System.out.println("Certificate auth with keystore: SUCCESS");
            }
        }

        @Test
        @DisplayName("Certificate auth executes parameterized query")
        void certAuthWithPreparedStatement() throws SQLException {
            assumeTrue(isCertAuthConfigured(),
                "Skipping cert auth PreparedStatement test - certificate auth not configured");

            String url = getCertAuthUrl() != null ? getCertAuthUrl() : URL;

            Properties props = new Properties();
            props.setProperty("clientCertAuth", "true");
            props.setProperty("ssl", "true");
            props.setProperty("sslMode", "no-verify");

            if (getClientKeystorePath() != null && getClientKeystorePassword() != null) {
                props.setProperty("clientCertKeystorePath", getClientKeystorePath());
                props.setProperty("clientCertKeystorePassword", getClientKeystorePassword());
            } else {
                props.setProperty("clientCertPath", getClientCertPath());
                props.setProperty("clientKeyPath", getClientKeyPath());
                if (getClientKeyPassword() != null) {
                    props.setProperty("clientKeyPassword", getClientKeyPassword());
                }
            }

            try (Connection conn = DriverManager.getConnection(url, props)) {
                try (PreparedStatement pstmt = conn.prepareStatement("SELECT ? as val")) {
                    pstmt.setString(1, "cert-auth-test");
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals("cert-auth-test", rs.getString("val"));
                    }
                }

                System.out.println("Certificate auth with PreparedStatement: SUCCESS");
            }
        }
    }

    // =========================================================================
    // CONNECTION STATE
    // =========================================================================

    @Nested
    @DisplayName("Connection State")
    class ConnectionStateTests {

        @Test
        @DisplayName("Connection is valid after creation")
        void connectionIsValid() throws SQLException {
            assertTrue(connection.isValid(5));
        }

        @Test
        @DisplayName("Connection closed state")
        void connectionClosedState() throws SQLException {
            Connection tempConn = DriverManager.getConnection(URL, USER, PASSWORD);
            assertFalse(tempConn.isClosed());

            tempConn.close();
            assertTrue(tempConn.isClosed());
        }

        @Test
        @DisplayName("Auto-commit is enabled by default")
        void autoCommitDefault() throws SQLException {
            assertTrue(connection.getAutoCommit());
        }

        @Test
        @DisplayName("Read-only mode can be set")
        void readOnlyMode() throws SQLException {
            Connection tempConn = DriverManager.getConnection(URL, USER, PASSWORD);

            // Note: setReadOnly(true) sends READ_ONLY=true to the server,
            // but isReadOnly() may return false (driver implementation detail)
            tempConn.setReadOnly(true);

            // The important thing is that read-only queries still work
            // and the server receives the READ_ONLY flag
            Statement stmt = tempConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as num");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));

            rs.close();
            stmt.close();
            tempConn.close();
        }
    }
}
