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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;

/**
 * Provides a handle into the SDK based on the {@link ConnectionCoordinate}.
 */
public class ConnectionHandle {

  private final Cluster cluster;

  ConnectionHandle(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Run a N1QL query against the cluster.
   *
   * @param statement the statement to execute.
   * @param options the query options.
   * @return the {@link QueryResult} in a blocking fashion.
   */
  public QueryResult query(final String statement, final QueryOptions options) {
    return cluster.query(statement, options);
  }

  /**
   * Run an Analytics query against the cluster.
   *
   * @param statement the statement to execute.
   * @param options the analytics options.
   * @return the {@link AnalyticsResult} in a blocking fashion.
   */
  public AnalyticsResult analyticsQuery(final String statement, final AnalyticsOptions options) {
    return cluster.analyticsQuery(statement, options);
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

}
