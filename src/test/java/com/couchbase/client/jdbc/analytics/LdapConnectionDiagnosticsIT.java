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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Base64;

/**
 * Diagnostic test to help identify the correct Analytics service endpoint.
 *
 * This test tries common Analytics ports and paths to help identify
 * where the Analytics service is actually running.
 */
@DisplayName("LDAP Connection Diagnostics")
class LdapConnectionDiagnosticsIT {

    private static final String HOST = System.getProperty("ldap.host", "localhost");
    private static final String USER = System.getProperty("ldap.user", "testuser");
    private static final String PASSWORD = System.getProperty("ldap.password", "password");

    // Common Analytics ports
    private static final int[] PORTS = {
        9000,   // Your specified port
        8095,   // Default Analytics HTTP port
        18095,  // Default Analytics HTTPS port
        9600,   // Alternative port sometimes used in tests
        8091    // Couchbase Web Console (for reference)
    };

    // Common Analytics paths
    private static final String[] PATHS = {
        "/api/v1/request",              // Standard Analytics API path
        "/analytics/service",           // Alternative path
        "/query/service",               // Query service path
        "/"                             // Root path
    };

    @Test
    @DisplayName("Diagnose Analytics service endpoint")
    void diagnoseEndpoint() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("ANALYTICS SERVICE ENDPOINT DIAGNOSTICS");
        System.out.println("=".repeat(80));
        System.out.println("Host: " + HOST);
        System.out.println("User: " + USER);
        System.out.println("\nTesting common Analytics service endpoints...\n");

        boolean foundEndpoint = false;

        for (int port : PORTS) {
            System.out.println("-".repeat(80));
            System.out.println("Testing port: " + port);
            System.out.println("-".repeat(80));

            for (String path : PATHS) {
                String urlStr = "http://" + HOST + ":" + port + path;
                try {
                    System.out.print("  Testing: " + urlStr + " ... ");

                    URL url = new URL(urlStr);
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("POST");
                    conn.setRequestProperty("Content-Type", "application/json");

                    // Add Basic Auth header
                    String auth = USER + ":" + PASSWORD;
                    String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                    conn.setRequestProperty("Authorization", "Basic " + encodedAuth);

                    // Send a simple test query
                    conn.setDoOutput(true);
                    String testQuery = "{\"statement\":\"SELECT 1\"}";
                    conn.getOutputStream().write(testQuery.getBytes(StandardCharsets.UTF_8));

                    conn.setConnectTimeout(3000);
                    conn.setReadTimeout(3000);

                    int responseCode = conn.getResponseCode();

                    if (responseCode == 200) {
                        System.out.println("✓ SUCCESS (HTTP " + responseCode + ")");
                        foundEndpoint = true;

                        // Read response
                        BufferedReader in = new BufferedReader(
                            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                        String inputLine;
                        StringBuilder response = new StringBuilder();
                        while ((inputLine = in.readLine()) != null) {
                            response.append(inputLine);
                        }
                        in.close();

                        System.out.println("    Response preview: " +
                            response.substring(0, Math.min(200, response.length())) + "...");

                        // Test JDBC connection
                        String jdbcUrl = "jdbc:couchbase:analytics://" + HOST + ":" + port;
                        System.out.println("\n    Trying JDBC connection: " + jdbcUrl);
                        try (Connection c = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
                            System.out.println("    ✓ JDBC connection successful!");
                            System.out.println("\n" + "=".repeat(80));
                            System.out.println("✓ FOUND WORKING ENDPOINT!");
                            System.out.println("  Use this JDBC URL: " + jdbcUrl);
                            System.out.println("=".repeat(80));
                        }

                    } else if (responseCode == 401 || responseCode == 403) {
                        System.out.println("✗ Auth failed (HTTP " + responseCode + ")");
                        System.out.println("    Endpoint exists but authentication failed");
                    } else if (responseCode == 404) {
                        System.out.println("✗ Not found (HTTP " + responseCode + ")");
                    } else {
                        System.out.println("? HTTP " + responseCode);

                        // Try to read error response
                        try {
                            BufferedReader in = new BufferedReader(
                                new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8));
                            String inputLine;
                            StringBuilder response = new StringBuilder();
                            while ((inputLine = in.readLine()) != null && response.length() < 200) {
                                response.append(inputLine);
                            }
                            in.close();
                            System.out.println("    Response: " + response);
                        } catch (Exception ignored) {
                        }
                    }

                    conn.disconnect();

                } catch (java.net.ConnectException e) {
                    System.out.println("✗ Connection refused");
                } catch (java.net.SocketTimeoutException e) {
                    System.out.println("✗ Timeout");
                } catch (Exception e) {
                    System.out.println("✗ Error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
            System.out.println();
        }

        if (!foundEndpoint) {
            System.out.println("\n" + "=".repeat(80));
            System.out.println("⚠ NO WORKING ANALYTICS ENDPOINT FOUND");
            System.out.println("=".repeat(80));
            System.out.println("\nPossible issues:");
            System.out.println("1. Analytics service is not running");
            System.out.println("2. Analytics service is on a different port");
            System.out.println("3. Firewall is blocking connections");
            System.out.println("4. LDAP authentication is not configured on the server");
            System.out.println("\nTo check Analytics service status:");
            System.out.println("  curl http://localhost:8091/pools/default (requires admin credentials)");
            System.out.println("=".repeat(80));
        }
    }
}
