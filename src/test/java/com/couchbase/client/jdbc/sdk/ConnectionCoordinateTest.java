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

/*
    @ai-generated tool=cursor model=claude-opus-4.5
 */
package com.couchbase.client.jdbc.sdk;

import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ConnectionCoordinate} client certificate authentication.
 */
class ConnectionCoordinateTest {

  @Test
  void testPasswordAuthentication() {
    Properties props = new Properties();
    ConnectionCoordinate coord = ConnectionCoordinate.create(
      "localhost", "user", "password", props, Duration.ZERO
    );

    assertFalse(coord.isCertificateAuth());
    assertInstanceOf(PasswordAuthenticator.class, coord.authenticator());
    assertEquals("localhost", coord.connectionString());
  }

  @Test
  void testCertificateAuthWithoutRequiredProperties() {
    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");

    assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
  }

  @Test
  void testCertificateAuthWithKeystoreWrongOrMissingPassword(@TempDir Path tempDir) throws Exception {
    Path keystorePath = tempDir.resolve("test.p12");
    String password = "testpass";

    // Generate keystore with password
    ProcessBuilder pb = new ProcessBuilder(
      "keytool", "-genkeypair",
      "-alias", "test",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "1",
      "-keystore", keystorePath.toString(),
      "-storepass", password,
      "-storetype", "PKCS12",
      "-dname", "CN=Test"
    );
    Process p = pb.start();
    assertEquals(0, p.waitFor(), "keytool failed");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_KEYSTORE_PATH.getName(), keystorePath.toString());
    // Note: NOT setting clientCertKeystorePassword - password is optional but keystore has one

    // Should fail during keystore loading (not with our validation error)
    // because the keystore requires a password
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
    // Error should be from SDK/keystore loading, not from our validation
    assertTrue(ex.getMessage().contains("Failed to create certificate authenticator"),
      "Expected keystore loading error: " + ex.getMessage());
  }

  @Test
  void testCertificateAuthWithPemMissingKeyPath() {
    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), "/path/to/cert.pem");

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
    assertTrue(ex.getMessage().contains("clientKeyPath") || ex.getMessage().contains("clientCertKeystorePath"));
  }

  @Test
  void testCertificateAuthWithKeystore(@TempDir Path tempDir) throws Exception {
    Path keystorePath = tempDir.resolve("test.p12");
    String password = "testpass";

    // Generate keystore using keytool
    ProcessBuilder pb = new ProcessBuilder(
      "keytool", "-genkeypair",
      "-alias", "test",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "1",
      "-keystore", keystorePath.toString(),
      "-storepass", password,
      "-storetype", "PKCS12",
      "-dname", "CN=Test"
    );
    Process p = pb.start();
    assertEquals(0, p.waitFor(), "keytool failed");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_KEYSTORE_PATH.getName(), keystorePath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_KEYSTORE_PASSWORD.getName(), password);

    ConnectionCoordinate coord = ConnectionCoordinate.create(
      "localhost", "user", "password", props, Duration.ZERO
    );

    assertTrue(coord.isCertificateAuth());
    assertInstanceOf(CertificateAuthenticator.class, coord.authenticator());
  }

  @Test
  void testCertificateAuthWithPemFiles(@TempDir Path tempDir) throws Exception {
    Path keystorePath = tempDir.resolve("test.p12");
    Path certPath = tempDir.resolve("client.crt");
    Path keyPath = tempDir.resolve("client.key");
    String password = "testpass";

    // Generate keystore
    ProcessBuilder pb1 = new ProcessBuilder(
      "keytool", "-genkeypair",
      "-alias", "test",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "1",
      "-keystore", keystorePath.toString(),
      "-storepass", password,
      "-storetype", "PKCS12",
      "-dname", "CN=Test"
    );
    assertEquals(0, pb1.start().waitFor(), "keytool genkeypair failed");

    // Export certificate
    ProcessBuilder pb2 = new ProcessBuilder(
      "keytool", "-exportcert",
      "-alias", "test",
      "-keystore", keystorePath.toString(),
      "-storepass", password,
      "-rfc",
      "-file", certPath.toString()
    );
    assertEquals(0, pb2.start().waitFor(), "keytool exportcert failed");

    // Export private key using openssl
    ProcessBuilder pb3 = new ProcessBuilder(
      "openssl", "pkcs12",
      "-in", keystorePath.toString(),
      "-nocerts",
      "-nodes",
      "-passin", "pass:" + password,
      "-out", keyPath.toString()
    );
    assertEquals(0, pb3.start().waitFor(), "openssl export key failed");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), certPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PATH.getName(), keyPath.toString());

    ConnectionCoordinate coord = ConnectionCoordinate.create(
      "localhost", "user", "password", props, Duration.ZERO
    );

    assertTrue(coord.isCertificateAuth());
    assertInstanceOf(CertificateAuthenticator.class, coord.authenticator());
  }

  @Test
  void testInvalidPemKeyContent(@TempDir Path tempDir) throws Exception {
    Path certPath = tempDir.resolve("client.crt");
    Path keyPath = tempDir.resolve("client.key");

    // Create a dummy certificate (we just need any valid cert for this test)
    Path keystorePath = tempDir.resolve("test.p12");
    ProcessBuilder pb1 = new ProcessBuilder(
      "keytool", "-genkeypair", "-alias", "test", "-keyalg", "RSA", "-keysize", "2048",
      "-validity", "1", "-keystore", keystorePath.toString(), "-storepass", "testpass",
      "-storetype", "PKCS12", "-dname", "CN=Test"
    );
    assertEquals(0, pb1.start().waitFor());

    ProcessBuilder pb2 = new ProcessBuilder(
      "keytool", "-exportcert", "-alias", "test", "-keystore", keystorePath.toString(),
      "-storepass", "testpass", "-rfc", "-file", certPath.toString()
    );
    assertEquals(0, pb2.start().waitFor());

    // Write invalid key content (corrupted base64 data)
    Files.writeString(keyPath, "-----BEGIN PRIVATE KEY-----\ninvalid_base64_data\n-----END PRIVATE KEY-----");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), certPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PATH.getName(), keyPath.toString());

    // Should fail with an error about invalid/corrupted key content
    assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
  }

  @Test
  void testInvalidCertificateFile(@TempDir Path tempDir) throws Exception {
    Path certPath = tempDir.resolve("client.crt");
    Path keyPath = tempDir.resolve("client.key");
    Files.writeString(certPath, "not a certificate");
    Files.writeString(keyPath, "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), certPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PATH.getName(), keyPath.toString());

    assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
  }

  @Test
  void testNonExistentKeystoreFile() {
    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_KEYSTORE_PATH.getName(), "/nonexistent/keystore.p12");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_KEYSTORE_PASSWORD.getName(), "password");

    assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
  }

  @Test
  void testNonExistentPemFiles() {
    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), "/nonexistent/cert.pem");
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PATH.getName(), "/nonexistent/key.pem");

    assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
  }

  @Test
  void testCertificateAuthWithEncryptedPemKey(@TempDir Path tempDir) throws Exception {
    Path keystorePath = tempDir.resolve("test.p12");
    Path certPath = tempDir.resolve("client.crt");
    Path keyPath = tempDir.resolve("client.key");
    String keystorePassword = "testpass";
    String keyPassword = "keypassword";

    // Generate keystore
    ProcessBuilder pb1 = new ProcessBuilder(
      "keytool", "-genkeypair",
      "-alias", "test",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "1",
      "-keystore", keystorePath.toString(),
      "-storepass", keystorePassword,
      "-storetype", "PKCS12",
      "-dname", "CN=Test"
    );
    assertEquals(0, pb1.start().waitFor(), "keytool genkeypair failed");

    // Export certificate
    ProcessBuilder pb2 = new ProcessBuilder(
      "keytool", "-exportcert",
      "-alias", "test",
      "-keystore", keystorePath.toString(),
      "-storepass", keystorePassword,
      "-rfc",
      "-file", certPath.toString()
    );
    assertEquals(0, pb2.start().waitFor(), "keytool exportcert failed");

    // Export private key with encryption using openssl (PKCS#8 encrypted format)
    ProcessBuilder pb3 = new ProcessBuilder(
      "openssl", "pkcs12",
      "-in", keystorePath.toString(),
      "-nocerts",
      "-passin", "pass:" + keystorePassword,
      "-passout", "pass:" + keyPassword,
      "-out", keyPath.toString()
    );
    assertEquals(0, pb3.start().waitFor(), "openssl export encrypted key failed");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), certPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PATH.getName(), keyPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PASSWORD.getName(), keyPassword);

    ConnectionCoordinate coord = ConnectionCoordinate.create(
      "localhost", "user", "password", props, Duration.ZERO
    );

    assertTrue(coord.isCertificateAuth());
    assertInstanceOf(CertificateAuthenticator.class, coord.authenticator());
  }

  @Test
  void testEncryptedPemKeyWithoutPassword(@TempDir Path tempDir) throws Exception {
    Path keystorePath = tempDir.resolve("test.p12");
    Path certPath = tempDir.resolve("client.crt");
    Path keyPath = tempDir.resolve("client.key");
    String keystorePassword = "testpass";
    String keyPassword = "keypassword";

    // Generate keystore
    ProcessBuilder pb1 = new ProcessBuilder(
      "keytool", "-genkeypair",
      "-alias", "test",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "1",
      "-keystore", keystorePath.toString(),
      "-storepass", keystorePassword,
      "-storetype", "PKCS12",
      "-dname", "CN=Test"
    );
    assertEquals(0, pb1.start().waitFor(), "keytool genkeypair failed");

    // Export certificate
    ProcessBuilder pb2 = new ProcessBuilder(
      "keytool", "-exportcert",
      "-alias", "test",
      "-keystore", keystorePath.toString(),
      "-storepass", keystorePassword,
      "-rfc",
      "-file", certPath.toString()
    );
    assertEquals(0, pb2.start().waitFor(), "keytool exportcert failed");

    // Export private key with encryption using openssl (PKCS#8 encrypted format)
    ProcessBuilder pb3 = new ProcessBuilder(
      "openssl", "pkcs12",
      "-in", keystorePath.toString(),
      "-nocerts",
      "-passin", "pass:" + keystorePassword,
      "-passout", "pass:" + keyPassword,
      "-out", keyPath.toString()
    );
    assertEquals(0, pb3.start().waitFor(), "openssl export encrypted key failed");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), certPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PATH.getName(), keyPath.toString());
    // Note: NOT setting clientKeyPassword

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
    assertTrue(ex.getMessage().contains("password") || ex.getMessage().contains("encrypted"),
      "Error should mention missing password: " + ex.getMessage());
  }

  @Test
  void testEncryptedPemKeyWithWrongPassword(@TempDir Path tempDir) throws Exception {
    Path keystorePath = tempDir.resolve("test.p12");
    Path certPath = tempDir.resolve("client.crt");
    Path keyPath = tempDir.resolve("client.key");
    String keystorePassword = "testpass";
    String keyPassword = "keypassword";

    // Generate keystore
    ProcessBuilder pb1 = new ProcessBuilder(
      "keytool", "-genkeypair",
      "-alias", "test",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "1",
      "-keystore", keystorePath.toString(),
      "-storepass", keystorePassword,
      "-storetype", "PKCS12",
      "-dname", "CN=Test"
    );
    assertEquals(0, pb1.start().waitFor(), "keytool genkeypair failed");

    // Export certificate
    ProcessBuilder pb2 = new ProcessBuilder(
      "keytool", "-exportcert",
      "-alias", "test",
      "-keystore", keystorePath.toString(),
      "-storepass", keystorePassword,
      "-rfc",
      "-file", certPath.toString()
    );
    assertEquals(0, pb2.start().waitFor(), "keytool exportcert failed");

    // Export private key with encryption using openssl (PKCS#8 encrypted format)
    ProcessBuilder pb3 = new ProcessBuilder(
      "openssl", "pkcs12",
      "-in", keystorePath.toString(),
      "-nocerts",
      "-passin", "pass:" + keystorePassword,
      "-passout", "pass:" + keyPassword,
      "-out", keyPath.toString()
    );
    assertEquals(0, pb3.start().waitFor(), "openssl export encrypted key failed");

    Properties props = new Properties();
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_AUTH.getName(), "true");
    props.setProperty(CouchbaseDriverProperty.CLIENT_CERT_PATH.getName(), certPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PATH.getName(), keyPath.toString());
    props.setProperty(CouchbaseDriverProperty.CLIENT_KEY_PASSWORD.getName(), "wrongpassword");

    assertThrows(IllegalArgumentException.class, () ->
      ConnectionCoordinate.create("localhost", "user", "password", props, Duration.ZERO)
    );
  }
}
