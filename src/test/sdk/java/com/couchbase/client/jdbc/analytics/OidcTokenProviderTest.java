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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for {@link OidcTokenProvider}: OIDC discovery of the token endpoint, the OAuth2
 * client-credentials token fetch, response parsing, and caching/refresh behaviour. Uses an
 * in-process HTTP server as a mock identity provider (serving both a discovery document and a token
 * endpoint), so no Keycloak or analytics server is required.
 */
class OidcTokenProviderTest {

  private HttpServer server;
  private String issuerUrl; // what callers pass as idpUrl
  private String tokenUrl;  // the endpoint advertised by the discovery document
  private final AtomicInteger tokenRequests = new AtomicInteger();
  private final AtomicInteger discoveryRequests = new AtomicInteger();
  private final List<String> requestBodies = new CopyOnWriteArrayList<>();
  // Body the mock IdP returns for the next token request.
  private final AtomicReference<String> responseBody = new AtomicReference<>();
  private final AtomicInteger responseStatus = new AtomicInteger(200);
  // Status the discovery endpoint returns (set < 200 / >= 300 to exercise the fallback path).
  private final AtomicInteger discoveryStatus = new AtomicInteger(200);

  @BeforeEach
  void startMockIdp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    int port = server.getAddress().getPort();
    issuerUrl = "http://127.0.0.1:" + port;
    tokenUrl = issuerUrl + "/token";

    server.createContext("/.well-known/openid-configuration", exchange -> {
      discoveryRequests.incrementAndGet();
      byte[] resp = ("{\"issuer\":\"" + issuerUrl + "\",\"token_endpoint\":\"" + tokenUrl + "\"}")
          .getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(discoveryStatus.get(), resp.length);
      exchange.getResponseBody().write(resp);
      exchange.close();
    });

    server.createContext("/token", exchange -> {
      // Only POSTs are real token requests; stray discovery probes (GET) get a 404 so they neither
      // count nor return a token. This mirrors a real token endpoint having no discovery document.
      if (!"POST".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
        return;
      }
      tokenRequests.incrementAndGet();
      byte[] reqBytes = exchange.getRequestBody().readAllBytes();
      requestBodies.add(new String(reqBytes, StandardCharsets.UTF_8));
      byte[] resp = responseBody.get().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(responseStatus.get(), resp.length);
      exchange.getResponseBody().write(resp);
      exchange.close();
    });
    server.start();
  }

  @AfterEach
  void stopMockIdp() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void fetchesTokenWithClientCredentialsGrant() throws IOException {
    responseBody.set("{\"access_token\":\"jwt-abc\",\"expires_in\":300,\"token_type\":\"Bearer\"}");
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "my-client", "my-secret");

    assertEquals("jwt-abc", provider.getToken());
    assertEquals(1, tokenRequests.get());

    // The request body must be a client-credentials grant carrying the (URL-encoded) credentials.
    String body = requestBodies.get(0);
    assertTrue(body.contains("grant_type=client_credentials"), body);
    assertTrue(body.contains("client_id=my-client"), body);
    assertTrue(body.contains("client_secret=my-secret"), body);
  }

  @Test
  void discoversTokenEndpointFromIssuer() throws IOException {
    // Caller passes only the issuer; the provider must discover token_endpoint and POST there.
    responseBody.set("{\"access_token\":\"discovered\",\"expires_in\":300}");
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "c", "s");

    assertEquals("discovered", provider.getToken());
    assertEquals(1, discoveryRequests.get(), "issuer must be resolved via the discovery document");
    assertEquals(1, tokenRequests.get(), "token must be fetched from the discovered endpoint");
  }

  @Test
  void resolvesDiscoveryOnlyOnce() throws IOException {
    // expires_in absent -> token re-fetched every call, but discovery must be resolved exactly once.
    responseBody.set("{\"access_token\":\"no-exp\"}");
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "c", "s");

    provider.getToken();
    provider.getToken();
    provider.getToken();
    assertEquals(3, tokenRequests.get(), "each call should fetch when expires_in is absent");
    assertEquals(1, discoveryRequests.get(), "discovery must be resolved once and cached");
  }

  @Test
  void fallsBackToIdpUrlWhenDiscoveryUnavailable() throws IOException {
    // Caller passes a literal token endpoint with no discovery document under it: the probe 404s,
    // and the provider must fall back to POSTing the credentials to idpUrl directly.
    responseBody.set("{\"access_token\":\"fallback\",\"expires_in\":300}");
    OidcTokenProvider provider = new OidcTokenProvider(tokenUrl, "c", "s");

    assertEquals("fallback", provider.getToken());
    assertEquals(1, tokenRequests.get());
    assertEquals(0, discoveryRequests.get(), "no discovery document exists under the token endpoint");
  }

  @Test
  void cachesTokenUntilNearExpiry() throws IOException {
    // expires_in=300s, refresh skew=30s -> cached for ~270s. Clock fixed -> all calls hit cache.
    responseBody.set("{\"access_token\":\"jwt-cached\",\"expires_in\":300}");
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "c", "s", () -> 0L);

    for (int i = 0; i < 10; i++) {
      assertEquals("jwt-cached", provider.getToken());
    }
    assertEquals(1, tokenRequests.get(), "10 getToken() calls should trigger exactly one IdP fetch");
  }

  @Test
  void refreshesAfterExpiry() throws IOException {
    long[] now = {0L};
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "c", "s", () -> now[0]);

    responseBody.set("{\"access_token\":\"token-1\",\"expires_in\":300}");
    assertEquals("token-1", provider.getToken());
    assertEquals(1, tokenRequests.get());

    // Still within the cache window (300s - 30s skew = 270s): no new fetch.
    now[0] = 200_000L; // 200s
    assertEquals("token-1", provider.getToken());
    assertEquals(1, tokenRequests.get());

    // Past the refresh point: a new token is fetched and returned.
    responseBody.set("{\"access_token\":\"token-2\",\"expires_in\":300}");
    now[0] = 280_000L; // 280s > 270s refresh threshold
    assertEquals("token-2", provider.getToken());
    assertEquals(2, tokenRequests.get());
  }

  @Test
  void refetchesEveryCallWhenExpiresInAbsent() throws IOException {
    // No expires_in -> no caching; every getToken() call must hit the IdP.
    responseBody.set("{\"access_token\":\"no-exp\"}");
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "c", "s");

    assertEquals("no-exp", provider.getToken());
    assertEquals("no-exp", provider.getToken());
    assertEquals("no-exp", provider.getToken());
    assertEquals(3, tokenRequests.get(), "each call should fetch when expires_in is absent");
  }

  @Test
  void throwsWhenAccessTokenMissing() {
    responseBody.set("{\"token_type\":\"Bearer\",\"expires_in\":300}");
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "c", "s");

    IOException ex = assertThrows(IOException.class, provider::getToken);
    assertTrue(ex.getMessage().contains("access_token"), ex.getMessage());
  }

  @Test
  void throwsOnNonSuccessStatus() {
    responseBody.set("{\"error\":\"unauthorized_client\"}");
    responseStatus.set(401);
    OidcTokenProvider provider = new OidcTokenProvider(issuerUrl, "c", "bad-secret");

    IOException ex = assertThrows(IOException.class, provider::getToken);
    assertTrue(ex.getMessage().contains("401"), ex.getMessage());
  }

  @Test
  void parseExtractsAccessTokenAndExpiresIn() throws IOException {
    byte[] json = "{\"access_token\":\"abc\",\"expires_in\":120}".getBytes(StandardCharsets.UTF_8);
    OidcTokenProvider.Token t = OidcTokenProvider.parse(json, "http://idp");
    assertNotNull(t);
    assertEquals("abc", t.accessToken);
    assertEquals(120L, t.expiresInSeconds);
  }

  @Test
  void parseTokenEndpointReadsDiscoveryDocument() throws IOException {
    byte[] json = ("{\"issuer\":\"https://idp\",\"authorization_endpoint\":\"https://idp/auth\","
        + "\"token_endpoint\":\"https://idp/token\"}").getBytes(StandardCharsets.UTF_8);
    assertEquals("https://idp/token", OidcTokenProvider.parseTokenEndpoint(json));
  }

  @Test
  void parseTokenEndpointReturnsNullWhenAbsent() throws IOException {
    byte[] json = "{\"issuer\":\"https://idp\"}".getBytes(StandardCharsets.UTF_8);
    assertNull(OidcTokenProvider.parseTokenEndpoint(json));
  }
}
