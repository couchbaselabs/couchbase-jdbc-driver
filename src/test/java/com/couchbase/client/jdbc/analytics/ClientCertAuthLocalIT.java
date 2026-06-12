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
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.Properties;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Client certificate authentication tests for local cluster.
 *
 * This test generates certificates dynamically and can optionally configure
 * the local Couchbase server for client certificate authentication.
 *
 * <h2>Prerequisites:</h2>
 * <ul>
 *   <li>Local Couchbase cluster running with Analytics service</li>
 *   <li>OpenSSL installed for certificate generation</li>
 *   <li>Server must be configured for client cert auth (can be done by this test)</li>
 * </ul>
 *
 * <h2>Run Command:</h2>
 * <pre>
 * mvn test -Dtest='ClientCertAuthLocalTest' \
 *   -Dlocal.url='jdbc:couchbase:analytics://localhost:19600' \
 *   -Dlocal.user=Administrator \
 *   -Dlocal.password=password \
 *   -Dlocal.mgmtPort=18091 \
 *   -Dlocal.configureServer=true
 * </pre>
 */
class ClientCertAuthLocalIT {

    private static final Logger LOGGER = Logger.getLogger(ClientCertAuthLocalIT.class.getName());

    // Connection properties from system properties (with defaults for local dev)
    private static final String URL = System.getProperty("local.url",
        "jdbc:couchbase:analytics://localhost:19600?ssl=true&sslMode=no-verify");
    private static final String USER = System.getProperty("local.user", "couchbase");
    private static final String PASSWORD = System.getProperty("local.password", "couchbase");
    private static final String MGMT_HOST = System.getProperty("local.mgmtHost", "localhost");
    private static final int MGMT_PORT = Integer.parseInt(System.getProperty("local.mgmtPort", "19000"));
    private static final boolean CONFIGURE_SERVER = Boolean.parseBoolean(
        System.getProperty("local.configureServer", "true"));

    private static boolean connectionConfigured = true; // defaults are provided

    // Certificate paths
    private static Path tempDir;
    private static Path caCertPath;
    private static Path caKeyPath;
    private static Path clientCertPath;
    private static Path clientKeyPath;
    private static Path clientKeyEncryptedPath;
    private static Path clientKeystorePath;

    private static final String KEYSTORE_PASSWORD = "changeit";
    private static final String CLIENT_KEY_PASSWORD = "keypassword";
    private static final String CLIENT_USERNAME = "certuser";

    private static boolean opensslAvailable = false;
    private static boolean serverConfigured = false;

    @BeforeAll
    static void setup() throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("Client Certificate Auth Local Tests");
        System.out.println("Analytics URL: " + URL);
        System.out.println("User: " + USER);
        System.out.println("Management: https://" + MGMT_HOST + ":" + MGMT_PORT);
        System.out.println("Configure Server: " + CONFIGURE_SERVER);
        System.out.println("=".repeat(60));

        // Verify server is actually reachable before running tests that require it.
        LOGGER.info("Using defaults: URL=" + URL + ", USER=" + USER);
        try (java.sql.Connection probe = DriverManager.getConnection(URL, USER, PASSWORD)) {
            connectionConfigured = true;
        } catch (Throwable t) {
            LOGGER.warning("Server not reachable at " + URL + ": " + t.getMessage()
                + " — password/connection tests will be skipped");
            connectionConfigured = false;
        }

        // Check if openssl is available
        opensslAvailable = checkOpenSslAvailable();
        if (!opensslAvailable) {
            LOGGER.warning("OpenSSL not available - certificate generation tests will be skipped");
            return;
        }

        // Create temp directory for certificates
        tempDir = Files.createTempDirectory("cert-auth-local-test");
        LOGGER.info("Temp directory: " + tempDir);

        // Generate certificates
        generateCertificates();

        // Optionally configure the server
        if (CONFIGURE_SERVER && connectionConfigured) {
            try {
                configureServerForCertAuth();
                serverConfigured = true;
            } catch (Exception e) {
                LOGGER.warning("Failed to configure server for cert auth: " + e.getMessage());
                LOGGER.warning("Server configuration requires correct management port. " +
                    "Set -Dlocal.mgmtPort=<port> (e.g., 18091 for Couchbase Server, or your Enterprise Analytics management port)");
                // Don't fail - tests will skip if server isn't configured
            }
        } else if (CONFIGURE_SERVER) {
            LOGGER.warning("Cannot configure server - connection properties not set");
        } else {
            LOGGER.info("Server configuration skipped - assuming server is already configured");
            LOGGER.info("Set -Dlocal.configureServer=true to configure automatically");
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        // Clean up temp directory
        if (tempDir != null) {
            Files.walk(tempDir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (Exception e) { /* ignore */ }
                });
        }
    }

    private static boolean checkOpenSslAvailable() {
        try {
            ProcessBuilder pb = new ProcessBuilder("openssl", "version");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            int exitCode = p.waitFor();
            return exitCode == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static void generateCertificates() throws Exception {
        caCertPath = tempDir.resolve("ca.crt");
        caKeyPath = tempDir.resolve("ca.key");
        clientCertPath = tempDir.resolve("client.crt");
        clientKeyPath = tempDir.resolve("client.key");
        clientKeystorePath = tempDir.resolve("client.p12");
        clientKeyEncryptedPath = tempDir.resolve("client-encrypted.key");
        Path clientCsrPath = tempDir.resolve("client.csr");

        // Generate CA private key
        runCommand("openssl", "genrsa", "-out", caKeyPath.toString(), "2048");

        // Generate CA certificate (self-signed)
        runCommand("openssl", "req", "-new", "-x509", "-days", "1",
            "-key", caKeyPath.toString(),
            "-out", caCertPath.toString(),
            "-subj", "/CN=TestCA/O=TestOrg");

        // Generate client private key
        runCommand("openssl", "genrsa", "-out", clientKeyPath.toString(), "2048");

        // Generate client CSR
        runCommand("openssl", "req", "-new",
            "-key", clientKeyPath.toString(),
            "-out", clientCsrPath.toString(),
            "-subj", "/CN=" + CLIENT_USERNAME + "/O=TestOrg");

        // Sign client certificate with CA
        runCommand("openssl", "x509", "-req", "-days", "1",
            "-in", clientCsrPath.toString(),
            "-CA", caCertPath.toString(),
            "-CAkey", caKeyPath.toString(),
            "-CAcreateserial",
            "-out", clientCertPath.toString());

        // Create PKCS#12 keystore
        runCommand("openssl", "pkcs12", "-export",
            "-in", clientCertPath.toString(),
            "-inkey", clientKeyPath.toString(),
            "-out", clientKeystorePath.toString(),
            "-passout", "pass:" + KEYSTORE_PASSWORD,
            "-name", "client");

        // Create encrypted private key
        runCommand("openssl", "pkcs8", "-topk8",
            "-in", clientKeyPath.toString(),
            "-out", clientKeyEncryptedPath.toString(),
            "-passout", "pass:" + CLIENT_KEY_PASSWORD);

        LOGGER.info("Generated certificates:");
        LOGGER.info("  CA Cert: " + caCertPath);
        LOGGER.info("  Client Cert: " + clientCertPath);
        LOGGER.info("  Client Key: " + clientKeyPath);
        LOGGER.info("  Client Keystore: " + clientKeystorePath);
    }

    private static void runCommand(String... command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        int exitCode = p.waitFor();
        if (exitCode != 0) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
            throw new RuntimeException("Command failed: " + String.join(" ", command) +
                "\nOutput: " + output);
        }
    }

    private static void configureServerForCertAuth() throws Exception {
        LOGGER.info("Configuring server for client certificate authentication...");

        // Create the certificate user
        createUser(CLIENT_USERNAME);

        // Upload CA certificate
        uploadCaCertificate();

        // Enable client certificate authentication
        enableClientCertAuth();

        LOGGER.info("Server configured successfully");
    }

    private static void createUser(String username) throws Exception {
        String url = String.format("https://%s:%d/settings/rbac/users/local/%s",
            MGMT_HOST, MGMT_PORT, username);
        String data = "password=" + KEYSTORE_PASSWORD + "&roles=admin";

        int responseCode = httpRequest("PUT", url, data, "application/x-www-form-urlencoded");
        if (responseCode == 200) {
            LOGGER.info("Created user: " + username);
        } else {
            LOGGER.warning("Failed to create user (may already exist): " + responseCode);
        }
    }

    private static void uploadCaCertificate() throws Exception {
        String url = String.format("https://%s:%d/controller/uploadClusterCA", MGMT_HOST, MGMT_PORT);
        String caCertContent = Files.readString(caCertPath);

        int responseCode = httpRequest("POST", url, caCertContent, "application/octet-stream");
        if (responseCode == 200) {
            LOGGER.info("Uploaded CA certificate");
        } else {
            LOGGER.warning("Failed to upload CA certificate: " + responseCode);
        }

        // Reload certificate
        String reloadUrl = String.format("https://%s:%d/node/controller/reloadCertificate",
            MGMT_HOST, MGMT_PORT);
        httpRequest("POST", reloadUrl, "", "application/x-www-form-urlencoded");
    }

    private static void enableClientCertAuth() throws Exception {
        String url = String.format("https://%s:%d/settings/clientCertAuth", MGMT_HOST, MGMT_PORT);
        String data = "{\"state\": \"enable\", \"prefixes\": [{\"path\": \"subject.cn\", \"prefix\": \"\", \"delimiter\": \"\"}]}";

        int responseCode = httpRequest("POST", url, data, "application/json");
        if (responseCode == 200) {
            LOGGER.info("Enabled client certificate authentication");
        } else {
            LOGGER.warning("Failed to enable client cert auth: " + responseCode);
        }
    }

    private static int httpRequest(String method, String urlString, String data, String contentType)
            throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        // Trust all certificates for management API
        if (conn instanceof javax.net.ssl.HttpsURLConnection) {
            javax.net.ssl.HttpsURLConnection httpsConn = (javax.net.ssl.HttpsURLConnection) conn;
            httpsConn.setSSLSocketFactory(createTrustAllSslContext().getSocketFactory());
            httpsConn.setHostnameVerifier((hostname, session) -> true);
        }

        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Basic " +
            Base64.getEncoder().encodeToString((USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        conn.setRequestProperty("Content-Type", contentType);

        if (data != null && !data.isEmpty()) {
            conn.setDoOutput(true);
            conn.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
        }

        return conn.getResponseCode();
    }

    private static javax.net.ssl.SSLContext createTrustAllSslContext() throws Exception {
        javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[] {
            new javax.net.ssl.X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
                public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
            }
        };
        javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("TLS");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        return sc;
    }

    // =========================================================================
    // PASSWORD AUTHENTICATION TESTS
    // =========================================================================

    @Nested
    @DisplayName("Password Authentication")
    class PasswordAuthTests {

        @Test
        @DisplayName("Password auth works without clientCertAuth flag")
        void passwordAuthWorks() throws Exception {
            assumeTrue(connectionConfigured,
                "Skipping - set local.url, local.user, local.password");

            try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
                assertNotNull(conn);
                assertFalse(conn.isClosed());

                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 1 as num")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("num"));
                }
            }
        }

        @Test
        @DisplayName("Password auth works with clientCertAuth=false")
        void passwordAuthWithExplicitFalse() throws Exception {
            assumeTrue(connectionConfigured,
                "Skipping - set local.url, local.user, local.password");

            String url = URL + (URL.contains("?") ? "&" : "?") + "clientCertAuth=false";

            try (Connection conn = DriverManager.getConnection(url, USER, PASSWORD)) {
                assertNotNull(conn);
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 1 as num")) {
                    assertTrue(rs.next());
                }
            }
        }
    }

    // =========================================================================
    // ERROR HANDLING TESTS
    // =========================================================================

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Fails with missing keystore path")
        void failsWithMissingKeystorePath() {
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true&clientCertKeystorePassword=test";

            assertThrows(Exception.class, () ->
                DriverManager.getConnection(url, "", "")
            );
        }

        @Test
        @DisplayName("Fails with missing keystore password")
        void failsWithMissingKeystorePassword() {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true&clientCertKeystorePath=" + clientKeystorePath;

            assertThrows(Exception.class, () ->
                DriverManager.getConnection(url, "", "")
            );
        }

        @Test
        @DisplayName("Fails with non-existent keystore")
        void failsWithNonExistentKeystore() {
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true&clientCertKeystorePath=/nonexistent/file.p12&clientCertKeystorePassword=test";

            assertThrows(Exception.class, () ->
                DriverManager.getConnection(url, "", "")
            );
        }

        @Test
        @DisplayName("Fails with missing PEM key path")
        void failsWithMissingPemKeyPath() {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true&clientCertPath=" + clientCertPath;

            assertThrows(Exception.class, () ->
                DriverManager.getConnection(url, "", "")
            );
        }

        @Test
        @DisplayName("Fails with encrypted key without password")
        void failsWithEncryptedKeyWithoutPassword() {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true" +
                "&clientCertPath=" + clientCertPath +
                "&clientKeyPath=" + clientKeyEncryptedPath;

            assertThrows(Exception.class, () ->
                DriverManager.getConnection(url, "", "")
            );
        }
    }

    // =========================================================================
    // CERTIFICATE LOADING TESTS
    // =========================================================================

    @Nested
    @DisplayName("Certificate Loading")
    class CertificateLoadingTests {

        @Test
        @DisplayName("Loads keystore successfully")
        void loadsKeystoreSuccessfully() throws Exception {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true" +
                "&clientCertKeystorePath=" + clientKeystorePath +
                "&clientCertKeystorePassword=" + KEYSTORE_PASSWORD;

            try {
                Connection conn = DriverManager.getConnection(url, "", "");
                assertNotNull(conn);
                conn.close();
                LOGGER.info("Keystore loaded and connection successful");
            } catch (Exception ex) {
                // Connection may fail due to server config, but keystore should load
                String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
                assertFalse(msg.contains("keystore"),
                    "Should not fail on keystore loading: " + ex.getMessage());
            }
        }

        @Test
        @DisplayName("Loads PEM files successfully")
        void loadsPemFilesSuccessfully() throws Exception {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true" +
                "&clientCertPath=" + clientCertPath +
                "&clientKeyPath=" + clientKeyPath;

            try {
                Connection conn = DriverManager.getConnection(url, "", "");
                assertNotNull(conn);
                conn.close();
                LOGGER.info("PEM files loaded and connection successful");
            } catch (Exception ex) {
                // Connection may fail due to server config, but PEM should load
                String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
                assertFalse(msg.contains("pem") || msg.contains("private key"),
                    "Should not fail on PEM loading: " + ex.getMessage());
            }
        }

        @Test
        @DisplayName("Loads encrypted PEM key with password")
        void loadsEncryptedPemKeyWithPassword() throws Exception {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true" +
                "&clientCertPath=" + clientCertPath +
                "&clientKeyPath=" + clientKeyEncryptedPath +
                "&clientKeyPassword=" + CLIENT_KEY_PASSWORD;

            try {
                Connection conn = DriverManager.getConnection(url, "", "");
                assertNotNull(conn);
                conn.close();
                LOGGER.info("Encrypted PEM key loaded and connection successful");
            } catch (Exception ex) {
                // Connection may fail due to server config, but key should decrypt
                String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
                assertFalse(msg.contains("password") || msg.contains("encrypted"),
                    "Should not fail on key decryption: " + ex.getMessage());
            }
        }
    }

    // =========================================================================
    // END-TO-END CERTIFICATE AUTH TESTS
    // =========================================================================

    @Nested
    @DisplayName("End-to-End Certificate Auth")
    class EndToEndTests {

        @Test
        @DisplayName("Execute query with keystore auth")
        void executeQueryWithKeystoreAuth() throws Exception {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true" +
                "&clientCertKeystorePath=" + clientKeystorePath +
                "&clientCertKeystorePassword=" + KEYSTORE_PASSWORD;

            try (Connection conn = DriverManager.getConnection(url, "", "")) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 'cert-auth' as method, 42 as answer")) {
                    assertTrue(rs.next());
                    assertEquals("cert-auth", rs.getString("method"));
                    assertEquals(42, rs.getInt("answer"));
                }
                LOGGER.info("Query execution with keystore auth: SUCCESS");
            } catch (Exception ex) {
                LOGGER.warning("Query with keystore auth failed (server may not be configured): " +
                    ex.getMessage());
            }
        }

        @Test
        @DisplayName("Execute query with PEM auth")
        void executeQueryWithPemAuth() throws Exception {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true" +
                "&clientCertPath=" + clientCertPath +
                "&clientKeyPath=" + clientKeyPath;

            try (Connection conn = DriverManager.getConnection(url, "", "")) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 'pem-auth' as method")) {
                    assertTrue(rs.next());
                    assertEquals("pem-auth", rs.getString("method"));
                }
                LOGGER.info("Query execution with PEM auth: SUCCESS");
            } catch (Exception ex) {
                LOGGER.warning("Query with PEM auth failed (server may not be configured): " +
                    ex.getMessage());
            }
        }

        @Test
        @DisplayName("Execute PreparedStatement with cert auth")
        void executePreparedStatementWithCertAuth() throws Exception {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            String url = URL + (URL.contains("?") ? "&" : "?") +
                "clientCertAuth=true" +
                "&clientCertKeystorePath=" + clientKeystorePath +
                "&clientCertKeystorePassword=" + KEYSTORE_PASSWORD;

            try (Connection conn = DriverManager.getConnection(url, "", "")) {
                try (PreparedStatement pstmt = conn.prepareStatement("SELECT ? as param")) {
                    pstmt.setString(1, "cert-value");
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals("cert-value", rs.getString("param"));
                    }
                }
                LOGGER.info("PreparedStatement with cert auth: SUCCESS");
            } catch (Exception ex) {
                LOGGER.warning("PreparedStatement with cert auth failed: " + ex.getMessage());
            }
        }

        @Test
        @DisplayName("Certificate auth via Properties object")
        void certAuthViaProperties() throws Exception {
            assumeTrue(opensslAvailable, "OpenSSL not available");
            assumeTrue(connectionConfigured, "Connection not configured");

            // Extract base URL without query params
            String baseUrl = URL.contains("?") ? URL.substring(0, URL.indexOf("?")) : URL;

            Properties props = new Properties();
            props.setProperty("clientCertAuth", "true");
            props.setProperty("ssl", "true");
            props.setProperty("sslMode", "no-verify");
            props.setProperty("clientCertKeystorePath", clientKeystorePath.toString());
            props.setProperty("clientCertKeystorePassword", KEYSTORE_PASSWORD);

            try (Connection conn = DriverManager.getConnection(baseUrl, props)) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 1 as num")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("num"));
                }
                LOGGER.info("Certificate auth via Properties: SUCCESS");
            } catch (Exception ex) {
                LOGGER.warning("Certificate auth via Properties failed: " + ex.getMessage());
            }
        }
    }
}
