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

package com.couchbase.client.jdbc.sdk;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;
import com.couchbase.client.jdbc.analytics.ClientCertificates;

import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A {@link ConnectionCoordinate} provides a "pointer" to a cluster managed by the SDK.
 */
public class ConnectionCoordinate {

  private final String connectionString;
  private final String username;
  private final String password;
  private final Authenticator authenticator;
  private final Properties properties;
  private final Duration connectTimeout;
  private final boolean certificateAuth;
  private final boolean plainSaslAuth;

  /**
   * Creates a new Connection Coordinate. Automatically determines the authentication method
   * based on the properties - uses client certificate authentication if clientCertAuth=true,
   * otherwise uses username/password authentication.
   *
   * @param connectionString the SDK connection string.
   * @param username the SDK username (used only for password authentication).
   * @param password the SDK password (used only for password authentication).
   * @param properties custom SDK properties.
   * @param connectTimeout the SDK connection timeout.
   * @return the created coordinate.
   */
  public static ConnectionCoordinate create(String connectionString, String username, String password,
                                            Properties properties, Duration connectTimeout) {
    boolean clientCertAuth = Boolean.parseBoolean(CouchbaseDriverProperty.CLIENT_CERT_AUTH.get(properties));
    if (clientCertAuth) {
      return createWithCertificateFromProperties(connectionString, properties, connectTimeout);
    }
    return new ConnectionCoordinate(connectionString, username, password, properties, connectTimeout);
  }

  /**
   * @ai-generated tool=cursor model=claude-opus-4.5
   * Creates a new Connection Coordinate with client certificate authentication.
   * Supports both keystore (PKCS12/JKS) and PEM file formats.
   */
  private static ConnectionCoordinate createWithCertificateFromProperties(String connectionString,
                                                                         Properties properties,
                                                                         Duration connectTimeout) {
    // First check for keystore-based configuration
    String keystorePath = CouchbaseDriverProperty.CLIENT_CERT_KEYSTORE_PATH.get(properties);
    if (keystorePath != null && !keystorePath.isEmpty()) {
      return createFromKeystore(connectionString, keystorePath, properties, connectTimeout);
    }

    // Otherwise check for PEM-based configuration
    String certPath = CouchbaseDriverProperty.CLIENT_CERT_PATH.get(properties);
    String keyPath = CouchbaseDriverProperty.CLIENT_KEY_PATH.get(properties);

    if (certPath != null && !certPath.isEmpty() && keyPath != null && !keyPath.isEmpty()) {
      return createFromPemFiles(connectionString, certPath, keyPath, properties, connectTimeout);
    }

    throw new IllegalArgumentException(
      "Client certificate authentication requires either clientCertKeystorePath, " +
      "or both clientCertPath and clientKeyPath to be set"
    );
  }

  /**
   * @ai-generated tool=cursor model=claude-opus-4.5
   * Creates a ConnectionCoordinate from a keystore file using SDK's CertificateAuthenticator.
   */
  private static ConnectionCoordinate createFromKeystore(String connectionString, String keystorePath,
                                                         Properties properties, Duration connectTimeout) {
    // Keystore password is optional - keystores can be created without a password
    String keystorePassword = CouchbaseDriverProperty.CLIENT_CERT_KEYSTORE_PASSWORD.get(properties);

    try {
      CertificateAuthenticator authenticator = CertificateAuthenticator.fromKeyStore(
        Paths.get(keystorePath),
        keystorePassword,
        Optional.empty()  // Use default keystore type
      );
      return new ConnectionCoordinate(connectionString, authenticator, properties, connectTimeout);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        "Failed to create certificate authenticator from keystore: " + e.getMessage(), e
      );
    }
  }

  /**
   * @ai-generated tool=cursor model=claude-opus-4.5
   * Creates a ConnectionCoordinate from PEM certificate and key files.
   */
  private static ConnectionCoordinate createFromPemFiles(String connectionString, String certPath,
                                                         String keyPath, Properties properties,
                                                         Duration connectTimeout) {
    try {
      String keyPassword = CouchbaseDriverProperty.CLIENT_KEY_PASSWORD.get(properties);
      PrivateKey privateKey = ClientCertificates.loadPrivateKeyFromPem(Paths.get(keyPath), keyPassword);
      List<X509Certificate> certChain = ClientCertificates.loadCertificatesFromPem(Paths.get(certPath));

      CertificateAuthenticator authenticator = CertificateAuthenticator.fromKey(
        privateKey,
        null,
        certChain
      );
      return new ConnectionCoordinate(connectionString, authenticator, properties, connectTimeout);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        "Failed to create certificate authenticator from PEM files: " + e.getMessage(), e
      );
    }
  }

  /**
   * Creates a coordinate with password authentication.
   * Uses PLAIN SASL authentication if enablePlainSaslAuth property is set to true.
   */
  private ConnectionCoordinate(String connectionString, String username, String password, Properties properties,
                               Duration connectTimeout) {
    this.connectionString = notNullOrEmpty(connectionString, "ConnectionString");
    this.username = username;
    this.password = password;
    this.properties = properties == null ? new Properties() : properties;
    this.connectTimeout = connectTimeout;
    this.certificateAuth = false;

    // Use PLAIN SASL authenticator if enablePlainSaslAuth is enabled (for LDAP, PAM, etc.)
    this.plainSaslAuth = Boolean.parseBoolean(CouchbaseDriverProperty.ENABLE_PLAIN_SASL_AUTH.get(this.properties));
    if (this.plainSaslAuth) {
      this.authenticator = notNull(PasswordAuthenticator.ldapCompatible(username, password), "Authenticator");
    } else {
      this.authenticator = notNull(PasswordAuthenticator.create(username, password), "Authenticator");
    }
  }

  /**
   * Creates a coordinate with certificate authentication.
   */
  private ConnectionCoordinate(String connectionString, CertificateAuthenticator authenticator,
                               Properties properties, Duration connectTimeout) {
    this.connectionString = notNullOrEmpty(connectionString, "ConnectionString");
    this.username = null;
    this.password = null;
    this.authenticator = notNull(authenticator, "Authenticator");
    this.properties = properties == null ? new Properties() : properties;
    this.connectTimeout = connectTimeout;
    this.certificateAuth = true;
    this.plainSaslAuth = false;
  }

  /**
   * Returns whether this coordinate uses certificate authentication.
   *
   * @return true if certificate authentication is used.
   */
  public boolean isCertificateAuth() {
    return certificateAuth;
  }

  /**
   * Returns whether this coordinate uses PLAIN SASL authentication.
   * PLAIN SASL is required for LDAP, PAM, and other external authentication systems.
   *
   * @return true if PLAIN SASL authentication is used.
   */
  public boolean isPlainSaslAuth() {
    return plainSaslAuth;
  }

  /**
   * Returns the connection string.
   *
   * @return the connection string.
   */
  public String connectionString() {
    return connectionString;
  }

  /**
   * The authenticator.
   *
   * @return the authenticator.
   */
  public Authenticator authenticator() {
    return authenticator;
  }

  /**
   * All current used properties.
   *
   * @return the used properties.
   */
  public Properties properties() {
    return properties;
  }

  /**
   * The used connect timeout.
   *
   * @return the used connect timeout.
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  @Override
  public String toString() {
    return "ConnectionCoordinate{" +
      "connectionString='" + connectionString + '\'' +
      ", authenticator=" + authenticator.getClass() +
      ", plainSaslAuth=" + plainSaslAuth +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConnectionCoordinate that = (ConnectionCoordinate) o;
    return certificateAuth == that.certificateAuth
      && plainSaslAuth == that.plainSaslAuth
      && Objects.equals(connectionString, that.connectionString)
      && Objects.equals(username, that.username)
      && Objects.equals(password, that.password)
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionString, username, password, properties, certificateAuth, plainSaslAuth);
  }
}
