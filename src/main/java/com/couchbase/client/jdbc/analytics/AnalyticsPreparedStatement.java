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
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.jdbc.sdk.ConnectionCoordinate;
import com.couchbase.client.jdbc.sdk.ConnectionHandle;
import com.couchbase.client.jdbc.sdk.ConnectionManager;
import com.couchbase.client.jdbc.util.Exceptions;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;

public class AnalyticsPreparedStatement extends AnalyticsStatement implements PreparedStatement {

  private final String sql;
  private final List<Object> args;
  private final AnalyticsConnection connection;
  private final List<AnalyticsColumn> columns;

  AnalyticsPreparedStatement(AnalyticsConnection connection, String sql, ConnectionCoordinate coordinate,
                             String catalog, String schema) throws SQLException {
    super(connection, coordinate, catalog, schema);
    this.sql = sql;
    this.connection = connection;

    AnalyticsCompilationInfo compilationInfo = precompileQuery(sql);
    this.args = Arrays.asList(new Object[compilationInfo.parameterCount()]);
    this.columns = compilationInfo.columns();
  }

  private AnalyticsCompilationInfo precompileQuery(String sql) throws SQLException {
    ConnectionHandle handle = ConnectionManager.INSTANCE.handle(connection.connectionCoordinate());

    byte[] content = JsonObject.create()
      .put("compile-only", true)
      .put("plan-format", "string")
      .put("client-type", "jdbc")
      .put("mode", "deferred")
      .put("signature", true)
      .put("statement", sql)
      .toBytes();

    CoreHttpResponse coreResponse = handle.rawAnalyticsQuery(Collections.emptyMap(), content);
    JsonObject decoded = JsonObject.fromJson(coreResponse.content());
    return new AnalyticsCompilationInfo(decoded.getObject("signature"), decoded.getObject("plans"));
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    AnalyticsOptions options = analyticsOptions().parameters(JsonArray.from(args));
    return buildResultSet(performAnalyticsQuery(sql, options));
  }

  @Override
  public boolean execute() throws SQLException {
    AnalyticsOptions options = analyticsOptions().parameters(JsonArray.from(args));
    AnalyticsResult currentResult = performAnalyticsQuery(sql, options);
    AnalyticsResultSet resultSet = buildResultSet(currentResult);
    setResultSet(resultSet);
    return currentResult.metaData().metrics().resultCount() > 0;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new AnalyticsResultSetMetaData(this, columns);
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return new AnalyticsParameterMetaData(this, args.size());
  }

  @Override
  public int executeUpdate() throws SQLException {
    throw Exceptions.unsupported("Updates are not supported");
  }

  private int argIndex(int parameterIndex) throws SQLException {
    boolean ok = 0 < parameterIndex && parameterIndex <= args.size();
    if (!ok) {
      throw new SQLException("Invalid parameter index: " + parameterIndex);
    }
    return parameterIndex - 1;
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    args.set(argIndex(parameterIndex), null);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void clearParameters() throws SQLException {
    for (int i = 0, n = args.size(); i < n; i++) {
      args.set(i, null);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    args.set(argIndex(parameterIndex), x);
  }

  @Override
  public void addBatch() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    args.set(argIndex(parameterIndex), value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

}
