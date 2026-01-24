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

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

import java.security.Security;

import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
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

  // Register Bouncy Castle provider for cryptographic operations
  static {
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
  }

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
      PrivateKey privateKey = loadPrivateKeyFromPem(Paths.get(keyPath), keyPassword);
      List<X509Certificate> certChain = loadCertificatesFromPem(Paths.get(certPath));

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
   * @ai-generated tool=cursor model=claude-opus-4.5
   * Loads a private key from a PEM file using Bouncy Castle.
   * Supports multiple formats: PKCS#8 (encrypted and unencrypted), PKCS#1 (RSA),
   * EC keys, and OpenSSL traditional encrypted format.
   *
   * @param keyPath the path to the PEM file containing the private key
   * @param password the password to decrypt the key (can be null for unencrypted keys)
   * @return the private key
   */
  private static PrivateKey loadPrivateKeyFromPem(Path keyPath, String password) throws Exception {
    try (PEMParser pemParser = new PEMParser(new FileReader(keyPath.toFile()))) {
      Object pemObject = pemParser.readObject();

      if (pemObject == null) {
        throw new IllegalArgumentException("No valid PEM content found in: " + keyPath);
      }

      JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

      // Handle PKCS#8 encrypted private key (BEGIN ENCRYPTED PRIVATE KEY)
      if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
        if (password == null || password.isEmpty()) {
          throw new IllegalArgumentException(
            "Private key is encrypted but no password provided. Set clientKeyPassword property."
          );
        }
        PKCS8EncryptedPrivateKeyInfo encryptedInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
        InputDecryptorProvider decryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
          .setProvider(BouncyCastleProvider.PROVIDER_NAME)
          .build(password.toCharArray());
        PrivateKeyInfo keyInfo = encryptedInfo.decryptPrivateKeyInfo(decryptorProvider);
        return converter.getPrivateKey(keyInfo);
      }

      // Handle OpenSSL traditional encrypted key pair (BEGIN RSA/DSA/EC PRIVATE KEY with encryption)
      if (pemObject instanceof PEMEncryptedKeyPair) {
        if (password == null || password.isEmpty()) {
          throw new IllegalArgumentException(
            "Private key is encrypted but no password provided. Set clientKeyPassword property."
          );
        }
        PEMEncryptedKeyPair encryptedKeyPair = (PEMEncryptedKeyPair) pemObject;
        PEMDecryptorProvider decryptorProvider = new JcePEMDecryptorProviderBuilder()
          .setProvider(BouncyCastleProvider.PROVIDER_NAME)
          .build(password.toCharArray());
        PEMKeyPair keyPair = encryptedKeyPair.decryptKeyPair(decryptorProvider);
        return converter.getPrivateKey(keyPair.getPrivateKeyInfo());
      }

      // Handle unencrypted key pair (BEGIN RSA/DSA/EC PRIVATE KEY without encryption)
      if (pemObject instanceof PEMKeyPair) {
        PEMKeyPair keyPair = (PEMKeyPair) pemObject;
        return converter.getPrivateKey(keyPair.getPrivateKeyInfo());
      }

      // Handle unencrypted PKCS#8 private key (BEGIN PRIVATE KEY)
      if (pemObject instanceof PrivateKeyInfo) {
        return converter.getPrivateKey((PrivateKeyInfo) pemObject);
      }

      throw new IllegalArgumentException(
        "Unsupported PEM format. Expected private key but found: " + pemObject.getClass().getSimpleName()
      );
    }
  }

  /**
   * @ai-generated tool=cursor model=claude-opus-4.5
   * Loads X.509 certificates from a PEM file using Bouncy Castle.
   * Supports multiple certificates in a single file (certificate chain).
   *
   * @param certPath the path to the PEM file containing the certificate(s)
   * @return the list of X.509 certificates
   */
  private static List<X509Certificate> loadCertificatesFromPem(Path certPath) throws Exception {
    List<X509Certificate> certs = new ArrayList<>();
    JcaX509CertificateConverter certConverter = new JcaX509CertificateConverter();

    try (PEMParser pemParser = new PEMParser(new FileReader(certPath.toFile()))) {
      Object pemObject;
      while ((pemObject = pemParser.readObject()) != null) {
        if (pemObject instanceof X509CertificateHolder) {
          X509CertificateHolder certHolder = (X509CertificateHolder) pemObject;
          certs.add(certConverter.getCertificate(certHolder));
        }
      }
    }

    if (certs.isEmpty()) {
      throw new IllegalArgumentException("No valid certificates found in: " + certPath);
    }
    return certs;
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
