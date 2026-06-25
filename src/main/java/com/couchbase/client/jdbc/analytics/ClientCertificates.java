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

import java.io.FileReader;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared client-certificate (mTLS) loading used by both the legacy java-client protocol and the
 * Analytics SDK protocol. Lives in the common source root so both build flavors compile it.
 *
 * <p>Supports PEM certificate/key files via Bouncy Castle: PKCS#8 (encrypted and unencrypted),
 * PKCS#1 (RSA), EC keys, and OpenSSL traditional encrypted format.
 */
public final class ClientCertificates {

  private static final BouncyCastleProvider BOUNCY_CASTLE_PROVIDER = new BouncyCastleProvider();

  private ClientCertificates() {
  }

  /**
   * Builds an in-memory {@link KeyStore} containing the private key and certificate chain loaded
   * from PEM files. The key entry is protected by {@code keyPassword} (empty string when null), so
   * pass that same password to whatever consumes the keystore.
   *
   * @param certPath path to the PEM file with the client certificate chain
   * @param keyPath path to the PEM file with the private key
   * @param keyPassword password to decrypt the key (null/empty for unencrypted keys)
   * @return a PKCS12 keystore with a single "client" key entry
   */
  public static KeyStore keyStoreFromPem(Path certPath, Path keyPath, String keyPassword) throws Exception {
    PrivateKey privateKey = loadPrivateKeyFromPem(keyPath, keyPassword);
    List<X509Certificate> certChain = loadCertificatesFromPem(certPath);

    char[] pwd = keyPassword == null ? new char[0] : keyPassword.toCharArray();
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry("client", privateKey, pwd, certChain.toArray(new Certificate[0]));
    return keyStore;
  }

  /**
   * Loads a private key from a PEM file using Bouncy Castle. Supports PKCS#8 (encrypted and
   * unencrypted), PKCS#1 (RSA), EC keys, and OpenSSL traditional encrypted format.
   *
   * @param keyPath the path to the PEM file containing the private key
   * @param password the password to decrypt the key (can be null for unencrypted keys)
   * @return the private key
   */
  public static PrivateKey loadPrivateKeyFromPem(Path keyPath, String password) throws Exception {
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
          .setProvider(BOUNCY_CASTLE_PROVIDER)
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
          .setProvider(BOUNCY_CASTLE_PROVIDER)
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
   * Loads X.509 certificates from a PEM file using Bouncy Castle. Supports multiple certificates in
   * a single file (certificate chain).
   *
   * @param certPath the path to the PEM file containing the certificate(s)
   * @return the list of X.509 certificates
   */
  public static List<X509Certificate> loadCertificatesFromPem(Path certPath) throws Exception {
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
}
