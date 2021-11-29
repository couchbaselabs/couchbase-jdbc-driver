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

import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.java.Cluster;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;

/**
 * Provides a handle into the SDK based on the {@link ConnectionCoordinate}.
 */
public class ConnectionHandle {

  private final ConnectionCoordinate coordinate;
  private final Cluster cluster;

  ConnectionHandle(Cluster cluster, ConnectionCoordinate coordinate) {
    this.cluster = cluster;
    this.coordinate = coordinate;
  }

  /**
   * Retrieves the raw cluster version as a string.
   *
   * @return the cluster version as a string if successful.
   * @throws SQLException if fetching the cluster version failed.
   */
  public String clusterVersion() throws SQLException {
    CoreHttpClient client = cluster.core().httpClient(RequestTarget.manager());

    CompletableFuture<CoreHttpResponse> exec = client
      .get(path("/pools"), CoreCommonOptions.DEFAULT)
      .build()
      .exec(cluster.core());

    try {
      JsonNode root = Mapper.decodeIntoTree(exec.get().content());
      return root.get("implementationVersion").asText();
    } catch (Exception e) {
      throw new SQLException("Failed to fetch cluster version", e);
    }
  }

  /**
   * Sends a raw analytics query, allows to be used where the regular API does not suffice.
   * <p>
   * It should really only be used if the primary query API cannot be used for some reason.
   *
   * @return the core response to use.
   */
  public CoreHttpResponse rawAnalyticsQuery(HttpMethod method, String path, Map<String, Object> headers, byte[] content,
                                            Duration timeout)
    throws SQLException {
    CoreHttpClient client = cluster.core().httpClient(RequestTarget.analytics());


    CoreCommonOptions options = CoreCommonOptions.of(timeout == null || timeout.isZero() ? null : timeout, null, null);

    CoreHttpRequest.Builder builder;
    switch (method) {
      case GET:
        builder = client.get(path(path), options);
        break;
      case DELETE:
        builder = client.delete(path(path), options);
        break;
      case POST:
        builder = client.post(path(path), options);
        if (content != null) {
          builder = builder.json(content);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported http verb: " + method);
    }

    if (headers != null) {
      for (Map.Entry<String, Object> header : headers.entrySet()) {
        builder = builder.header(header.getKey(), header.getValue());
      }
    }

    try {
      return builder.build().exec(cluster.core()).get();
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof CouchbaseException) {
        String ctx = ((CouchbaseException) ex.getCause()).context().exportAsString(Context.ExportFormat.JSON);
        throw new SQLException("Failed to perform analytics query: " + ctx, ex);
      } else {
        throw new SQLException("Failed to perform analytics query - cause: " + ex.getMessage(), ex);
      }
    } catch (Exception ex) {
      throw new SQLException("Failed to perform analytics query - cause: " + ex.getMessage(), ex);
    }
  }

  public void close() {
    ConnectionManager.INSTANCE.maybeClose(coordinate);
  }

  public enum HttpMethod {
    GET,
    POST,
    DELETE
  }

}
