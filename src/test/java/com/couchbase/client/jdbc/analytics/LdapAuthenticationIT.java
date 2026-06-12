/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test LDAP authentication with Couchbase Analytics JDBC Driver.
 *
 * <h2>Prerequisites:</h2>
 * <ul>
 *   <li>Couchbase server running with LDAP enabled</li>
 *   <li>LDAP user configured on the server</li>
 *   <li>Analytics service running on port 9000 (or configured port)</li>
 * </ul>
 *
 * <h2>Run Command:</h2>
 * <pre>
 * mvn test -Dtest=LdapAuthenticationTest \
 *   -Dldap.url='jdbc:couchbase:analytics://localhost:9000' \
 *   -Dldap.user='testuser' \
 *   -Dldap.password='password'
 * </pre>
 *
 * <h2>With SSL (Recommended for LDAP):</h2>
 * <pre>
 * mvn test -Dtest=LdapAuthenticationTest \
 *   -Dldap.url='jdbc:couchbase:analytics://localhost:9000?ssl=true&amp;sslMode=no-verify' \
 *   -Dldap.user='testuser' \
 *   -Dldap.password='password'
 * </pre>
 */
@DisplayName("LDAP Authentication Tests")
class LdapAuthenticationIT {

    // Test configuration from system properties
    private static final String URL = System.getProperty("ldap.url",
        "jdbc:couchbase:analytics://localhost:9600");
    private static final String USER = System.getProperty("ldap.user", "testuser");
    private static final String PASSWORD = System.getProperty("ldap.password", "password");

    @Test
    @DisplayName("Test basic LDAP authentication and connection")
    void testLdapConnection() throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("LDAP Authentication Test");
        System.out.println("JDBC URL: " + URL);
        System.out.println("User: " + USER);
        System.out.println("=".repeat(60));

        // Attempt to connect with LDAP credentials
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            assertNotNull(connection, "Connection should not be null");
            assertFalse(connection.isClosed(), "Connection should be open");

            System.out.println("✓ Successfully authenticated with LDAP credentials");
            System.out.println("✓ Connection established: " + connection);

            // Test basic query execution
            try (Statement stmt = connection.createStatement()) {
                System.out.println("\nExecuting test query: SELECT 1");
                try (ResultSet rs = stmt.executeQuery("SELECT 1 as test_value")) {
                    assertTrue(rs.next(), "Result set should have at least one row");
                    int value = rs.getInt("test_value");
                    assertEquals(1, value, "Query should return 1");
                    System.out.println("✓ Test query executed successfully, result: " + value);
                }
            }

            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓ LDAP Authentication Test PASSED");
            System.out.println("=".repeat(60));
        } catch (Exception e) {
            System.err.println("\n" + "=".repeat(60));
            System.err.println("✗ LDAP Authentication Test FAILED");
            System.err.println("Error: " + e.getMessage());
            System.err.println("=".repeat(60));
            throw e;
        }
    }

    @Test
    @DisplayName("Test LDAP authentication with metadata queries")
    void testLdapWithMetadata() throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("LDAP Authentication - Metadata Test");
        System.out.println("=".repeat(60));

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            System.out.println("✓ Connected with LDAP user: " + USER);

            // Get database metadata
            var metadata = connection.getMetaData();
            System.out.println("\nDatabase Metadata:");
            System.out.println("  - Database Product: " + metadata.getDatabaseProductName());
            System.out.println("  - Database Version: " + metadata.getDatabaseProductVersion());
            System.out.println("  - Driver Name: " + metadata.getDriverName());
            System.out.println("  - Driver Version: " + metadata.getDriverVersion());
            System.out.println("  - User Name: " + metadata.getUserName());

            // Execute a catalog query
            try (Statement stmt = connection.createStatement()) {
                System.out.println("\nExecuting catalog query...");
                try (ResultSet rs = stmt.executeQuery("SELECT VALUE dv FROM Metadata.`Dataverse` dv LIMIT 5")) {
                    int count = 0;
                    System.out.println("Dataverses found:");
                    while (rs.next()) {
                        count++;
                        System.out.println("  " + count + ". " + rs.getString(1));
                    }
                    System.out.println("✓ Found " + count + " dataverse(s)");
                }
            }

            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓ LDAP Metadata Test PASSED");
            System.out.println("=".repeat(60));
        }
    }

    @Test
    @DisplayName("Test LDAP authentication with prepared statement")
    void testLdapWithPreparedStatement() throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("LDAP Authentication - PreparedStatement Test");
        System.out.println("=".repeat(60));

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            System.out.println("✓ Connected with LDAP user: " + USER);

            // Test prepared statement with parameters
            String sql = "SELECT ? as param1, ? as param2, ? + ? as sum";
            try (var pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, "Hello");
                pstmt.setString(2, "LDAP");
                pstmt.setInt(3, 10);
                pstmt.setInt(4, 20);

                System.out.println("Executing prepared statement with parameters...");
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("Hello", rs.getString("param1"));
                    assertEquals("LDAP", rs.getString("param2"));
                    assertEquals(30, rs.getInt("sum"));
                    System.out.println("✓ Results: param1=" + rs.getString(1) +
                                     ", param2=" + rs.getString(2) +
                                     ", sum=" + rs.getInt(3));
                }
            }

            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓ LDAP PreparedStatement Test PASSED");
            System.out.println("=".repeat(60));
        }
    }

    @Test
    @DisplayName("Test LDAP authentication failure with wrong password")
    void testLdapAuthenticationFailure() throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("LDAP Authentication - Failure Test");
        System.out.println("Testing with incorrect password...");
        System.out.println("=".repeat(60));

        // Try to connect with wrong password
        assertThrows(Exception.class, () -> {
            try (Connection connection = DriverManager.getConnection(URL, USER, "wrongpassword")) {
                fail("Should have thrown an authentication exception");
            }
        }, "Connection with wrong password should fail");

        System.out.println("✓ Authentication correctly failed with wrong password");
        System.out.println("=".repeat(60));
    }
}
