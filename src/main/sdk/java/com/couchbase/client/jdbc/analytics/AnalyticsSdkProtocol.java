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

import com.couchbase.analytics.client.java.Cluster;
import com.couchbase.analytics.client.java.Credential;
import com.couchbase.analytics.client.java.InternalUnsupportedHttpClient;
import com.couchbase.analytics.client.java.internal.RawQueryMetadata;
import com.couchbase.client.jdbc.util.Golang;
import com.couchbase.client.jdbc.CouchbaseDriver;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;

/**
 * Implementation of the Analytics protocol using the Couchbase Analytics Java SDK.
 * Uses InternalUnsupportedHttpClient for HTTP requests (queries and cancel operations).
 */
public class AnalyticsSdkProtocol extends ADBProtocolBase {

  // OkHttp 5.x caps explicit timeouts at Integer.MAX_VALUE ms; passing null to the SDK's
  // opts.timeout() API means "use OkHttp default (0 = no limit)".
  // executeStreaming requires a non-null Duration, so we use the OkHttp 5.x maximum there.
  private static final Duration NO_TIMEOUT = null;
  private static final Duration NO_TIMEOUT_HTTP = Duration.ofMillis(Integer.MAX_VALUE);

  // Endpoint paths
  private static final String ACTIVE_REQUESTS_ENDPOINT_PATH = "/api/v1/active_requests";

  // Local constants for scan options (not in ADBProtocolBase)
  private static final String SCAN_WAIT = "scan_wait";
  private static final String SCAN_CONSISTENCY = "scan_consistency";

  private final Cluster cluster;
  private final InternalUnsupportedHttpClient httpClient;
  private final String scanConsistency;
  private final String scanWait;

  AnalyticsSdkProtocol(final Properties properties, final String hostname, final int port,
                       final ADBDriverContext driverContext, final Map<ADBDriverProperty, Object> params) {
    super(driverContext, params);

    String user = (String) ADBDriverProperty.Common.USER.fetchPropertyValue(params);
    String password = (String) ADBDriverProperty.Common.PASSWORD.fetchPropertyValue(params);
    String idpUrl = CouchbaseDriverProperty.IDP_URL.get(properties);
    String clientId = CouchbaseDriverProperty.CLIENT_ID.get(properties);
    String clientSecret = CouchbaseDriverProperty.CLIENT_SECRET.get(properties);
    String accessToken = CouchbaseDriverProperty.ACCESS_TOKEN.get(properties);

    // Parse scan wait
    String sw = CouchbaseDriverProperty.SCAN_WAIT.get(properties);
    if (sw == null || sw.isEmpty()) {
      scanWait = null;
    } else {
      try {
        // Validate the duration but forward the original string verbatim — re-serializing via
        // Duration.getSeconds() would silently truncate sub-second values (e.g. "500ms" -> "0s").
        Duration duration = Golang.parseDuration(sw);
        scanWait = duration.isZero() ? null : sw;
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException("Provided scanWait value \"" + sw + "\" is invalid");
      }
    }

    // Parse scan consistency
    String sc = CouchbaseDriverProperty.SCAN_CONSISTENCY.get(properties);
    if ("requestPlus".equals(sc)) {
      scanConsistency = "request_plus";
    } else if ("notBounded".equals(sc)) {
      scanConsistency = "not_bounded";
    } else if (sc == null || sc.isEmpty()) {
      scanConsistency = null;
    } else {
      throw new IllegalArgumentException("Provided scanConsistency value \"" + sc + "\" is invalid");
    }

    // Build connection string for Analytics SDK
    boolean ssl = Boolean.parseBoolean(CouchbaseDriverProperty.SSL.get(properties));
    String scheme = ssl ? "https" : "http";
    String connectionString = scheme + "://" + hostname + (port > 0 ? ":" + port : "");

    // Check SSL mode for certificate verification
    String sslMode = CouchbaseDriverProperty.SSL_MODE.get(properties);
    boolean noVerify = "no-verify".equals(sslMode);

    // Create Analytics SDK cluster
    Integer connectTimeout = (Integer) ADBDriverProperty.Common.CONNECT_TIMEOUT.fetchPropertyValue(params);
    Duration timeout = connectTimeout != null ? Duration.ofSeconds(connectTimeout) : Duration.ofSeconds(30);

    Credential credential;
    if (accessToken != null && !accessToken.isEmpty()) {
      // Interactive OAuth (Authorization Code flow): the host (e.g. Tableau) performs the browser
      // login and token refresh, and supplies the current access token per connection. We present
      // it directly as a bearer JWT.
      credential = Credential.ofJwt(accessToken);
    } else if (idpUrl != null && !idpUrl.isEmpty() && clientId != null && !clientId.isEmpty()) {
      // Non-interactive OAuth (Client Credentials flow): the driver fetches + caches the JWT itself.
      OidcTokenProvider tokenProvider = new OidcTokenProvider(idpUrl, clientId, clientSecret);
      credential = Credential.ofDynamic(() -> {
        try {
          return Credential.ofJwt(tokenProvider.getToken());
        } catch (Exception e) {
          throw new RuntimeException("Failed to fetch OIDC token from " + idpUrl + ": " + e, e);
        }
      });
    } else {
      credential = Credential.of(user, password);
    }

    this.cluster = Cluster.newInstance(
        connectionString,
        credential,
        opts -> {
          opts.timeout(t -> t.queryTimeout(NO_TIMEOUT))
              .timeout(t -> t.connectTimeout(timeout));
          // If SSL is enabled but no-verify mode, disable certificate verification
          if (ssl && noVerify) {
            opts.security(sec -> sec.disableServerCertificateVerification(true));
          }
        }
    );

    // Create HTTP client for direct API calls (cancel operations, streaming queries)
    this.httpClient = InternalUnsupportedHttpClient.from(cluster);
  }

  @Override
  public String connect() throws SQLException {
    return CouchbaseDriver.PRODUCT_NAME;
  }

  @Override
  public void close() {
    cluster.close();
  }

  @Override
  public boolean ping(final int timeoutSeconds) {
    Duration timeout = timeoutSeconds > 0 ? Duration.ofSeconds(timeoutSeconds) : NO_TIMEOUT;
    try {
      cluster.executeQuery("SELECT 1", opts -> opts.timeout(timeout));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public QueryServiceResponse submitStatement(final String sql, final List<?> args,
                                               final SubmitStatementOptions options) throws SQLException {

    if (getLogger().isLoggable(Level.FINE)) {
      getLogger().log(Level.FINE, String.format("%s { %s } with args { %s }",
          options.compileOnly ? "compile" : "execute", sql, args != null ? args : ""));
    }

    // Serialize request body directly to JsonGenerator (matching AnalyticsProtocol)
    byte[] requestBody;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
      try (com.fasterxml.jackson.core.JsonGenerator jsonGen =
          driverContext.getGenericObjectWriter().getFactory()
              .createGenerator(baos, com.fasterxml.jackson.core.JsonEncoding.UTF8)) {

      jsonGen.writeStartObject();
      jsonGen.writeStringField("statement", sql);

      // Standard options
      jsonGen.writeStringField(CLIENT_TYPE, CLIENT_TYPE_JDBC);
      jsonGen.writeBooleanField(SIGNATURE, true);
      jsonGen.writeStringField(PLAN_FORMAT, PLAN_FORMAT_STRING);
      jsonGen.writeNumberField(MAX_WARNINGS, maxWarnings);

      // Conditional options
      if (options.sqlCompatMode) {
        jsonGen.writeBooleanField(SQL_COMPAT, true);
      }
      if (options.compileOnly) {
        jsonGen.writeBooleanField(COMPILE_ONLY, true);
      }
      if (options.forceReadOnly) {
        jsonGen.writeBooleanField(READ_ONLY, true);
      }
      if (options.timeoutSeconds > 0) {
        jsonGen.writeStringField(TIMEOUT, options.timeoutSeconds + "s");
      }
      if (options.dataverseName != null) {
        jsonGen.writeStringField(DATAVERSE, options.dataverseName);
      }
      if (options.executionId != null) {
        jsonGen.writeStringField(CLIENT_CONTEXT_ID, options.executionId.toString());
      }
      if (scanWait != null) {
        jsonGen.writeStringField(SCAN_WAIT, scanWait);
      }
      if (scanConsistency != null) {
        jsonGen.writeStringField(SCAN_CONSISTENCY, scanConsistency);
      }

      // Serialize args using ADM format (matching AnalyticsProtocol for lossless precision)
      if (args != null && !args.isEmpty()) {
        jsonGen.writeFieldName(ARGS);
        driverContext.getAdmFormatObjectWriter().writeValue(jsonGen, args);
      }

      jsonGen.writeEndObject();
      } // closes JsonGenerator (flushes on close)
      requestBody = baos.toByteArray();
    } catch (Exception e) {
      throw new SQLException("Failed to serialize query request: " + e.getMessage(), e);
    }

    // Execute streaming query with lossless-adm=true (matching AnalyticsProtocol)
    // This preserves precision for decimals and uses ADM format which ADBRowStore can decode
    Duration timeout = options.timeoutSeconds > 0
        ? Duration.ofSeconds(options.timeoutSeconds)
        : NO_TIMEOUT_HTTP;

    try {
      // Build a JSON array of raw row bytes inline — avoids retaining Row objects.
      // NOTE: the entire result is materialised here because the SDK's executeStreaming only
      // returns the query metadata (signature/plans/status) after the whole row stream is
      // drained, and the JDBC core needs that metadata before fetchResult is called. The SDK
      // exposes no deferred-mode handle, so true row-at-a-time streaming is not possible here.
      ByteArrayOutputStream jsonArray = new ByteArrayOutputStream(8192);
      jsonArray.write('[');
      boolean[] firstRow = {true};

      RawQueryMetadata metadata = httpClient.executeStreaming(
          request -> request
              .path("/api/v1/request")
              .header("Accept", "application/json; charset=UTF-8; lossless-adm=true")
              .postJson(requestBody),
          timeout,
          row -> {
            if (!firstRow[0]) {
              jsonArray.write(',');
            }
            firstRow[0] = false;
            byte[] bytes = row.bytes();
            jsonArray.write(bytes, 0, bytes.length);
          },
          null
      );
      jsonArray.write(']');

      return buildQueryServiceResponse(metadata, jsonArray.toByteArray());

    } catch (Exception e) {
      if (e instanceof SQLException) {
        throw (SQLException) e;
      }
      throw new SQLException("Failed to execute analytics query: " + e.getMessage(), e);
    }
  }

  private QueryServiceResponse buildQueryServiceResponse(RawQueryMetadata metadata, byte[] resultsJson)
      throws IOException {
    QueryServiceResponse response = new QueryServiceResponse();

    // Map status string to enum
    if (metadata.status != null) {
      try {
        response.status = QueryServiceResponse.Status.valueOf(metadata.status.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        getLogger().log(Level.WARNING, "Unrecognised query status ''{0}'' from server; treating as SUCCESS",
            metadata.status);
        response.status = QueryServiceResponse.Status.SUCCESS;
      }
    }

    if (metadata.signature != null) {
      response.signature = driverContext.getGenericObjectReader()
          .forType(QueryServiceResponse.Signature.class)
          .readValue(metadata.signature);
    }
    if (metadata.plans != null) {
      response.plans = driverContext.getGenericObjectReader()
          .forType(QueryServiceResponse.Plans.class)
          .readValue(metadata.plans);
    }
    if (metadata.warnings != null) {
      response.warnings = driverContext.getGenericObjectReader()
          .forType(new com.fasterxml.jackson.core.type.TypeReference<List<QueryServiceResponse.Message>>() {})
          .readValue(metadata.warnings);
    }
    // Note: metadata.errors is never populated by the SDK parser (it throws QueryException instead),
    // so no error-mapping block is needed here.

    // For compile-only / EXPLAIN responses the base class reads the plan text directly out of
    // response.results (expecting String entries via fetchExplainOnlyResult), so decode the rows
    // here. Normal queries keep the raw JSON-array bytes, which fetchResult parses directly.
    if (isExplainOnly(response)) {
      response.results = driverContext.getGenericObjectReader()
          .forType(new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {})
          .readValue(resultsJson);
    } else {
      response.results = Collections.singletonList(resultsJson);
    }

    return response;
  }

  @Override
  public JsonParser fetchResult(QueryServiceResponse response, SubmitStatementOptions options) throws SQLException {
    try {
      if (response.results == null || response.results.isEmpty()) {
        return driverContext.getGenericObjectReader().getFactory().createParser("[]");
      }
      // results holds the pre-built JSON array from buildQueryServiceResponse
      byte[] jsonArray = (byte[]) response.results.get(0);
      return driverContext.getGenericObjectReader().getFactory().createParser(jsonArray);
    } catch (IOException e) {
      throw new SQLException("Failed to parse query results: " + e.getMessage(), e);
    }
  }

  @Override
  public void cancelRunningStatement(UUID uuid) throws SQLException {
    String cancelPath = ACTIVE_REQUESTS_ENDPOINT_PATH + "?" + CLIENT_CONTEXT_ID + "=" + uuid.toString();

    try (InternalUnsupportedHttpClient.Response response = httpClient.execute(
        request -> request
            .path(cancelPath)
            .delete(),
        Duration.ofSeconds(30)
    )) {
      if (response.httpStatusCode() < 200 || response.httpStatusCode() >= 300) {
        String body = response.bodyAsString();
        throw new SQLException("Failed to cancel running statement \"" + uuid +
            "\". Response: " + response.httpStatusCode() + " - " + body);
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("Failed to cancel running statement: " + e.getMessage(), e);
    }
  }
}
