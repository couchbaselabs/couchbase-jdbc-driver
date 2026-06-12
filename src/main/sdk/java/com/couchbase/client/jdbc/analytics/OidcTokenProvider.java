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

import com.fasterxml.jackson.core.JsonFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fetches and caches an OIDC access token via the OAuth2 client-credentials grant.
 *
 * <p>The Analytics SDK invokes the dynamic credential supplier on every HTTP request. When the
 * token endpoint returns {@code expires_in}, this provider caches the JWT and re-fetches it
 * {@value #REFRESH_SKEW_MS} ms before expiry. When {@code expires_in} is absent the token is
 * never cached and a fresh one is fetched on every call.
 *
 * <p>{@code idpUrl} is treated as the OIDC <em>issuer</em>: the provider resolves the token
 * endpoint via OIDC discovery ({@code <issuer>/.well-known/openid-configuration}), so any
 * standards-compliant IdP works from the issuer alone — no per-IdP token path needs to be known.
 * The discovered endpoint is resolved once and cached. If discovery fails transiently, the provider
 * falls back to {@code idpUrl} as the literal token endpoint for that call and retries discovery on
 * the next call. If the IdP genuinely has no discovery document, every call falls back to
 * {@code idpUrl}.
 */
final class OidcTokenProvider {

  private static final Logger LOGGER = Logger.getLogger(OidcTokenProvider.class.getName());

  // Shared, thread-safe factory — avoids allocating a new one per parse call.
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  // Refresh the cached token this many milliseconds before its advertised expiry.
  private static final long REFRESH_SKEW_MS = 30_000L;

  // Standard OIDC discovery document path appended to the issuer.
  private static final String DISCOVERY_PATH = "/.well-known/openid-configuration";

  private final String idpUrl;
  private final String clientId;
  private final String clientSecret;
  private final LongSupplier clock;

  private final Object lock = new Object();
  // cachedJwt and refreshAtMs are guarded by lock.
  private String cachedJwt;
  private long refreshAtMs;
  // Written via volatile once discovery succeeds; read without the lock after that.
  // Not set on fallback so a transient discovery failure retries on the next call.
  private volatile String tokenEndpoint;

  OidcTokenProvider(String idpUrl, String clientId, String clientSecret) {
    this(idpUrl, clientId, clientSecret, System::currentTimeMillis);
  }

  // Package-private constructor allowing tests to inject a controllable clock.
  OidcTokenProvider(String idpUrl, String clientId, String clientSecret, LongSupplier clock) {
    this.idpUrl = idpUrl;
    this.clientId = clientId != null ? clientId : "";
    this.clientSecret = clientSecret != null ? clientSecret : "";
    this.clock = clock;
  }

  /**
   * Returns a valid access token. When the IdP advertises {@code expires_in}, the token is cached
   * and reused until {@value #REFRESH_SKEW_MS} ms before expiry (capped at half the TTL for
   * short-lived tokens). When {@code expires_in} is absent, a fresh token is fetched on every call.
   */
  String getToken() throws IOException {
    // Fast path: serve from cache without I/O.
    synchronized (lock) {
      long now = clock.getAsLong();
      if (cachedJwt != null && now < refreshAtMs) {
        return cachedJwt;
      }
    }
    // Fetch outside the lock: network I/O must not block concurrent callers.
    // Two threads may fetch simultaneously on cache miss; that is acceptable —
    // results are idempotent and the race window is narrow.
    Token token = fetch();
    synchronized (lock) {
      long now = clock.getAsLong();
      if (token.expiresInSeconds > 0) {
        long ttlMs = token.expiresInSeconds * 1000L;
        // Cap skew at half the TTL so tokens with TTL < 2*REFRESH_SKEW_MS are still cached.
        long skew = Math.min(REFRESH_SKEW_MS, ttlMs / 2);
        cachedJwt = token.accessToken;
        refreshAtMs = now + ttlMs - skew;
      } else {
        // No expiry info from IdP — do not cache; re-fetch on every call.
        cachedJwt = null;
        refreshAtMs = 0;
      }
      return token.accessToken;
    }
  }

  private Token fetch() throws IOException {
    String endpoint = resolveTokenEndpoint();
    String body = "grant_type=client_credentials"
        + "&client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
        + "&client_secret=" + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);

    HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    conn.setRequestProperty("Accept", "application/json");
    conn.setDoOutput(true);
    conn.setConnectTimeout(10_000);
    conn.setReadTimeout(15_000);

    try (OutputStream os = conn.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
    }

    int status = conn.getResponseCode();
    if (status < 200 || status >= 300) {
      String errorBody = "";
      try (InputStream errorStream = conn.getErrorStream()) {
        if (errorStream != null) {
          byte[] bodyBytes = errorStream.readAllBytes();
          // Truncate to keep the message readable; full body rarely needed.
          int len = Math.min(bodyBytes.length, 512);
          errorBody = new String(bodyBytes, 0, len, StandardCharsets.UTF_8);
        }
      } catch (IOException drainFailed) { /* best-effort */ }
      IOException ex = new IOException("OIDC token request failed: HTTP " + status +
          " from " + endpoint + (errorBody.isEmpty() ? "" : " — " + errorBody));
      LOGGER.log(Level.WARNING, ex.getMessage());
      throw ex;
    }

    byte[] responseBytes;
    try (InputStream is = conn.getInputStream()) {
      responseBytes = is.readAllBytes();
    }

    return parse(responseBytes, endpoint);
  }

  /**
   * Resolves the token endpoint via OIDC discovery and caches it on success. Called outside
   * {@link #lock} — discovery involves blocking I/O and must not hold the cache lock.
   * On transient failure, returns {@code idpUrl} without caching so the next call retries.
   */
  private String resolveTokenEndpoint() {
    if (tokenEndpoint != null) {
      return tokenEndpoint;
    }
    String discovered = discoverTokenEndpoint();
    if (discovered != null) {
      // Volatile write — visible to all threads without a lock.
      tokenEndpoint = discovered;
      return discovered;
    }
    // Discovery failed or yielded no endpoint: fall back to idpUrl but do not cache,
    // so a transient failure (DNS blip, IdP restart) recovers on the next call.
    return idpUrl;
  }

  // Returns the discovered token_endpoint, or null if discovery is unavailable or fails.
  // Returning null (not idpUrl) lets the caller decide whether to cache.
  private String discoverTokenEndpoint() {
    String base = idpUrl.endsWith("/") ? idpUrl.substring(0, idpUrl.length() - 1) : idpUrl;
    String discoveryUrl = base.endsWith(DISCOVERY_PATH) ? base : base + DISCOVERY_PATH;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(discoveryUrl).openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/json");
      conn.setConnectTimeout(10_000);
      conn.setReadTimeout(15_000);

      int status = conn.getResponseCode();
      if (status >= 200 && status < 300) {
        byte[] bytes;
        try (InputStream is = conn.getInputStream()) {
          bytes = is.readAllBytes();
        }
        String endpoint = parseTokenEndpoint(bytes); // IOException caught by outer catch
        if (endpoint != null) {
          return endpoint;
        }
      } else {
        try (InputStream errorStream = conn.getErrorStream()) {
          if (errorStream != null) {
            errorStream.readAllBytes(); // drain for connection reuse
          }
        } catch (IOException drainFailed) { /* best-effort */ }
      }
    } catch (IOException e) {
      // Discovery unavailable or document unparseable — caller will use idpUrl as fallback.
      LOGGER.log(Level.FINE, "OIDC discovery unavailable for {0}, using idpUrl as token endpoint: {1}",
          new Object[]{idpUrl, e.getMessage()});
    }
    return null;
  }

  // Extracts the "token_endpoint" from an OIDC discovery document, or null if absent.
  static String parseTokenEndpoint(byte[] responseBytes) throws IOException {
    try (com.fasterxml.jackson.core.JsonParser parser = JSON_FACTORY.createParser(responseBytes)) {
      while (parser.nextToken() != null) {
        String field = parser.getCurrentName();
        if ("token_endpoint".equals(field) && parser.nextToken() != null) {
          return parser.getText();
        }
      }
    }
    return null;
  }

  // Parses access_token (required) and expires_in (optional) from a token endpoint response.
  static Token parse(byte[] responseBytes, String idpUrl) throws IOException {
    String accessToken = null;
    long expiresInSeconds = 0L;
    try (com.fasterxml.jackson.core.JsonParser parser = JSON_FACTORY.createParser(responseBytes)) {
      while (parser.nextToken() != null) {
        String field = parser.getCurrentName();
        if ("access_token".equals(field) && parser.nextToken() != null) {
          accessToken = parser.getText();
        } else if ("expires_in".equals(field) && parser.nextToken() != null) {
          // getText() works for both NUMBER and STRING tokens; handles non-standard IdPs
          // that serialize expires_in as a quoted string (e.g. "3600" instead of 3600).
          try {
            expiresInSeconds = Long.parseLong(parser.getText());
          } catch (NumberFormatException ignored) { /* malformed expires_in — treat as absent */ }
        }
      }
    }
    if (accessToken == null) {
      throw new IOException("OIDC token response from " + idpUrl + " missing 'access_token' field");
    }
    return new Token(accessToken, expiresInSeconds);
  }

  /** Holder for the parsed token endpoint response. */
  static final class Token {
    final String accessToken;
    final long expiresInSeconds; // 0 if the endpoint did not return "expires_in"

    Token(String accessToken, long expiresInSeconds) {
      this.accessToken = accessToken;
      this.expiresInSeconds = expiresInSeconds;
    }
  }
}
