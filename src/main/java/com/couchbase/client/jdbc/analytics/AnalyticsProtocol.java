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

import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.jdbc.CouchbaseDriver;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;
import com.couchbase.client.jdbc.sdk.ConnectionCoordinate;
import com.couchbase.client.jdbc.sdk.ConnectionHandle;
import com.couchbase.client.jdbc.sdk.ConnectionManager;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;

public class AnalyticsProtocol extends ADBProtocolBase {

  private static final String SCAN_WAIT = "scan_wait";
  private static final String SCAN_CONSISTENCY = "scan_consistency";

  private static final String QUERY_SERVICE_ENDPOINT_PATH = "/query/service";
  private static final String QUERY_RESULT_ENDPOINT_PATH = "/query/service/result";
  private static final String ACTIVE_REQUESTS_ENDPOINT_PATH = "/analytics/admin/active_requests";
  private static final String PING_ENDPOINT_PATH = "/admin/ping";

  private final ConnectionHandle connectionHandle;

  private final String scanConsistency;
  private final String scanWait;

  AnalyticsProtocol(final Properties properties, final String hostname, final int port,
                    final ADBDriverContext driverContext, final Map<ADBDriverProperty, Object> params) {
    super(driverContext, params);

    String user = (String) ADBDriverProperty.Common.USER.fetchPropertyValue(params);
    String password = (String) ADBDriverProperty.Common.PASSWORD.fetchPropertyValue(params);

    String sw = CouchbaseDriverProperty.SCAN_WAIT.get(properties);
    if (sw == null || sw.isEmpty()) {
      scanWait = null;
    } else {
      try {
        Duration duration = Golang.parseDuration(sw);
        scanWait = duration.isZero() ? null : sw;
      } catch (InvalidArgumentException ex) {
        throw new IllegalArgumentException("Provided scanWait value \"" + sw + "\" is invalid");
      }
    }

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

    String connectionString = hostname;
    if (port > 0) {
      connectionString = connectionString + ":" + port;
    }

    Integer connectTimeout = (Integer) ADBDriverProperty.Common.CONNECT_TIMEOUT.fetchPropertyValue(params);
    Duration parsedTimeout = connectTimeout != null ? Duration.ofSeconds(connectTimeout) : Duration.ZERO;

    this.connectionHandle = ConnectionManager.INSTANCE.handle(ConnectionCoordinate.create(
      connectionString, user, password, properties, parsedTimeout
    ));
  }

  @Override
  public String connect() throws SQLException {
    return CouchbaseDriver.PRODUCT_NAME + "/" + connectionHandle.clusterVersion();
  }

  @Override
  public void close() {
    connectionHandle.close();
  }

  @Override
  public boolean ping(final int timeoutSeconds) {
    try {
      CoreHttpResponse coreHttpResponse = connectionHandle.rawAnalyticsQuery(
        ConnectionHandle.HttpMethod.GET,
        PING_ENDPOINT_PATH,
        Collections.emptyMap(),
        null,
        Duration.ofSeconds(timeoutSeconds)
      );

      if (!coreHttpResponse.status().success()) {
        throw new SQLException("Failed to run ping. Response: " + coreHttpResponse);
      }

      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public QueryServiceResponse submitStatement(final String sql, final List<?> args, final SubmitStatementOptions options) throws SQLException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
    try {
      JsonGenerator jsonGen =
        driverContext.getGenericObjectWriter().getFactory().createGenerator(baos, JsonEncoding.UTF8);
      jsonGen.writeStartObject();
      jsonGen.writeStringField(CLIENT_TYPE, CLIENT_TYPE_JDBC);
      jsonGen.writeStringField(MODE, MODE_DEFERRED);
      jsonGen.writeStringField(STATEMENT, sql);
      jsonGen.writeBooleanField(SIGNATURE, true);
      jsonGen.writeStringField(PLAN_FORMAT, PLAN_FORMAT_STRING);
      jsonGen.writeNumberField(MAX_WARNINGS, maxWarnings);
      if (options.compileOnly) {
        jsonGen.writeBooleanField(COMPILE_ONLY, true);
      }
      if (options.forceReadOnly) {
        jsonGen.writeBooleanField(READ_ONLY, true);
      }
      if (options.sqlCompatMode) {
        jsonGen.writeBooleanField(SQL_COMPAT, true);
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
      if(scanWait != null && !scanWait.isEmpty()) {
        jsonGen.writeStringField(SCAN_WAIT, scanWait);
      }
      if (scanConsistency != null && !scanConsistency.isEmpty()) {
        jsonGen.writeStringField(SCAN_CONSISTENCY, scanConsistency);
      }
      if (args != null && !args.isEmpty()) {
        jsonGen.writeFieldName(ARGS);
        driverContext.getAdmFormatObjectWriter().writeValue(jsonGen, args);
      }
      jsonGen.writeEndObject();
      jsonGen.flush();
    } catch (InvalidDefinitionException e) {
      throw getErrorReporter().errorUnexpectedType(e.getType().getRawClass());
    } catch (IOException e) {
      throw getErrorReporter().errorInRequestGeneration(e);
    }

    Map<String, Object> headers = new HashMap<>();
    headers.put("Accept", "application/json; charset=UTF-8; lossless-adm=true");

    if (getLogger().isLoggable(Level.FINE)) {
      getLogger().log(Level.FINE, String.format("%s { %s } with args { %s }",
        options.compileOnly ? "compile" : "execute", sql, args != null ? args : ""));
    }

    try {
      CoreHttpResponse coreHttpResponse = connectionHandle.rawAnalyticsQuery(
        ConnectionHandle.HttpMethod.POST,
        QUERY_SERVICE_ENDPOINT_PATH,
        headers,
        baos.toByteArray(),
        getTimeout(options)
      );

      return driverContext.getGenericObjectReader()
        .forType(QueryServiceResponse.class)
        .readValue(coreHttpResponse.content());
    } catch (JsonProcessingException e) {
      throw getErrorReporter().errorInProtocol(e);
    } catch (IOException e) {
      throw getErrorReporter().errorInConnection(e);
    }
  }

  private Duration getTimeout(SubmitStatementOptions options) {
    // By default, JDBC mandates an infinite timeout, but the SDK will set it to 75s.
    // So if 0 from the adb layer, set it to MAX_VALUE to effectively simulate the
    // same duration.
    return options.timeoutSeconds > 0
      ? Duration.ofSeconds(options.timeoutSeconds)
      : Duration.ofNanos(Long.MAX_VALUE);
  }

  @Override
  public JsonParser fetchResult(QueryServiceResponse response, SubmitStatementOptions options) throws SQLException {
    int p = response.handle.lastIndexOf("/");
    if (p < 0) {
      throw new SQLNonTransientConnectionException("Protocol error - could not extract deferred ID");
    }
    String handlePath = response.handle.substring(p);

    CoreHttpResponse coreHttpResponse = connectionHandle.rawAnalyticsQuery(
      ConnectionHandle.HttpMethod.GET,
      QUERY_RESULT_ENDPOINT_PATH + handlePath,
      Collections.emptyMap(),
      null,
      getTimeout(options)
    );

    try {
      JsonParser parser = driverContext.getGenericObjectReader().getFactory().createParser(coreHttpResponse.content());

      if (!advanceToResults(parser)) {
        throw new SQLNonTransientConnectionException("Protocol error - could not advance to RESULT");
      }

      return parser;
    } catch (JsonProcessingException e) {
      throw getErrorReporter().errorInProtocol(e);
    } catch (IOException e) {
      throw getErrorReporter().errorInConnection(e);
    }
  }

  private boolean advanceToResults(JsonParser parser) throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      return false;
    }
    for (;;) {
      JsonToken token = parser.nextValue();
      if (token == null || token == JsonToken.END_OBJECT) {
        return false;
      }
      if (parser.currentName().equals(RESULTS)) {
        return token == JsonToken.START_ARRAY;
      } else if (token.isStructStart()) {
        parser.skipChildren();
      } else {
        parser.nextToken();
      }
    }
  }

  @Override
  public void cancelRunningStatement(UUID uuid) throws SQLException {
    CoreHttpResponse coreHttpResponse = connectionHandle.rawAnalyticsQuery(
      ConnectionHandle.HttpMethod.DELETE,
      ACTIVE_REQUESTS_ENDPOINT_PATH + "?" + CLIENT_CONTEXT_ID + "=" + uuid.toString(),
      Collections.emptyMap(),
      null,
      null
    );

    if (!coreHttpResponse.status().success()) {
      throw new SQLException("Failed to cancel running statement \""+uuid+"\". Response: " + coreHttpResponse);
    }
  }

}
