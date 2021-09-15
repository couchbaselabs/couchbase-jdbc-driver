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

import com.couchbase.client.jdbc.common.CommonConnection;
import com.couchbase.client.jdbc.common.CommonResultSet;
import com.couchbase.client.jdbc.sdk.ConnectionCoordinate;
import com.couchbase.client.jdbc.util.Exceptions;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class AnalyticsConnection extends CommonConnection {

  public AnalyticsConnection(final ConnectionCoordinate coordinate, String catalog, String schema) {
    super(coordinate, catalog, schema);
  }

  @Override
  public AnalyticsStatement createStatement() throws SQLException {
    return new AnalyticsStatement(this, connectionCoordinate(), getCatalog(), getSchema());
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    verifyResultSetProperties(resultSetType, resultSetConcurrency, CommonResultSet.HOLDABILITY);
    return createStatement();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    verifyResultSetProperties(resultSetType, resultSetConcurrency, resultSetHoldability);
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new AnalyticsPreparedStatement(this, sql, connectionCoordinate(), getCatalog(), getSchema());
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    verifyResultSetProperties(resultSetType, resultSetConcurrency, CommonResultSet.HOLDABILITY);
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    verifyResultSetProperties(resultSetType, resultSetConcurrency, resultSetHoldability);
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw Exceptions.unsupported();
  }

  private void verifyResultSetProperties(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    if (resultSetType != CommonResultSet.TYPE) {
      throw new SQLException("Unsupported ResultSetType " + resultSetType);
    }
    if (resultSetConcurrency != CommonResultSet.CONCURRENCY) {
      throw new SQLException("Unsupported ResultSetConcurrency " + resultSetConcurrency);
    }
    if (resultSetHoldability != CommonResultSet.HOLDABILITY) {
      throw new SQLException("Unsupported ResultSetHoldability " + resultSetHoldability);
    }
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (!autoCommit) {
      throw Exceptions.unsupported("Disabling AutoCommit is not supported");
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public void commit() throws SQLException {
    throw Exceptions.unsupported("Only AutoCommit support is available");
  }

  @Override
  public void rollback() throws SQLException {
    throw Exceptions.unsupported("Only AutoCommit support is available");
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new AnalyticsDatabaseMetaData(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (readOnly) {
      throw Exceptions.unsupported("Enabling readOnly is not supported");
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    if (level != Connection.TRANSACTION_READ_COMMITTED) {
      throw Exceptions.unsupported("The provided TransactionIsolation is not supported: " + level);
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return Collections.emptyMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    if (holdability != CommonResultSet.HOLDABILITY) {
      throw Exceptions.unsupported("The provided holdability is not supported: " + holdability);
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    return CommonResultSet.HOLDABILITY;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw Exceptions.unsupported("Not supported with AutoCommit");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return true;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new SQLClientInfoException("Failed", Collections.emptyMap(), Exceptions.unsupported());
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new SQLClientInfoException("Failed", Collections.emptyMap(), Exceptions.unsupported());
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new SQLClientInfoException("Failed", Collections.emptyMap(), Exceptions.unsupported());
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLClientInfoException("Failed", Collections.emptyMap(), Exceptions.unsupported());
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    // Doesn't do anything.
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO
  }
}