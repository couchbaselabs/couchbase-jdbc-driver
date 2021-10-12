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

import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.jdbc.common.CommonResultSet;
import com.couchbase.client.jdbc.sdk.ConnectionCoordinate;
import com.couchbase.client.jdbc.sdk.ConnectionHandle;
import com.couchbase.client.jdbc.sdk.ConnectionManager;
import com.couchbase.client.jdbc.util.Exceptions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class AnalyticsStatement implements Statement {

  private final AnalyticsConnection connection;
  private final ConnectionCoordinate coordinate;
  private final String schema;
  private final String catalog;

  private volatile AnalyticsResultSet currentResultSet = null;
  private volatile Duration queryTimeout = Duration.ofSeconds(75);
  private volatile int maxRows = 0;

  AnalyticsStatement(AnalyticsConnection connection, ConnectionCoordinate coordinate, String catalog, String schema) {
    this.connection = connection;
    this.coordinate = coordinate;
    this.schema = schema;
    this.catalog = catalog;
  }

  ResultSet empty() throws SQLException {
    return new AnalyticsResultSet(
      new AnalyticsResultSetMetaData(this, Collections.emptyList()),
      this,
      0,
      Collections.emptyList()
    );
  }

  ResultSet fromData(final List<LinkedHashMap<String, Object>> rows) throws SQLException {
    return new AnalyticsResultSet(
      new AnalyticsResultSetMetaData(this, Collections.emptyList()),
      this,
      0,
      rows
    );
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return buildResultSet(performAnalyticsQuery(sql, AnalyticsOptions.analyticsOptions()));
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    AnalyticsResult currentResult = performAnalyticsQuery(sql, AnalyticsOptions.analyticsOptions());
    currentResultSet = buildResultSet(currentResult);
    return currentResult.metaData().metrics().resultCount() > 0;
  }

  protected AnalyticsResult performAnalyticsQuery(String sql, AnalyticsOptions options) {
    ConnectionHandle handle = ConnectionManager.INSTANCE.handle(coordinate);
    return handle.analyticsQuery(
      sql,
      options
        .raw("client-type", "jdbc")
        .raw("signature", true)
        .timeout(queryTimeout)
    );
  }

  protected AnalyticsResultSet buildResultSet(AnalyticsResult result) throws SQLException {
    JsonObject signature = result.metaData().signature().orElse(null);
    JsonObject plans = result.metaData().plans().orElse(null);
    AnalyticsCompilationInfo compilationInfo = new AnalyticsCompilationInfo(signature, plans);

    return new AnalyticsResultSet(
      new AnalyticsResultSetMetaData(this, compilationInfo.columns()),
      this,
      maxRows,
      result.rowsAs(new TypeRef<LinkedHashMap<String, Object>>() {})
    );
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public int getMaxRows() throws SQLException {
    return maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return (int) queryTimeout.getSeconds();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    this.queryTimeout = Duration.ofSeconds(seconds);
  }

  @Override
  public void cancel() throws SQLException {

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw Exceptions.unsupported("Updates are not supported (statement is: " + sql + ")");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw Exceptions.unsupported("Updates are not supported (statement is: " + sql + ")");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw Exceptions.unsupported("Updates are not supported (statement is: " + sql + ")");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw Exceptions.unsupported("Updates are not supported (statement is: " + sql + ")");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return currentResultSet;
  }

  void setResultSet(AnalyticsResultSet resultSet) {
    this.currentResultSet = resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    // TODO
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != CommonResultSet.FETCH_DIRECTION) {
      throw Exceptions.unsupported("Unsupported FetchDirection " + direction);
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return CommonResultSet.FETCH_DIRECTION;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return CommonResultSet.CONCURRENCY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return CommonResultSet.TYPE;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return empty();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return CommonResultSet.HOLDABILITY;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO

    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    if (poolable) {
      throw Exceptions.unsupported("Pooling is not supported");
    }
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // TODO
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
