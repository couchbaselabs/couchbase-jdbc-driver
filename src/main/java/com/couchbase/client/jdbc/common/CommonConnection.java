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

package com.couchbase.client.jdbc.common;

import com.couchbase.client.jdbc.sdk.ConnectionCoordinate;
import com.couchbase.client.jdbc.util.Exceptions;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;

public abstract class CommonConnection implements Connection {

  private final ConnectionCoordinate connectionCoordinate;
  private volatile String schema;
  private volatile String catalog;

  protected CommonConnection(ConnectionCoordinate connectionCoordinate, String catalog, String schema) {
    this.connectionCoordinate = connectionCoordinate;
    this.catalog = catalog;
    this.schema = schema;
  }

  public ConnectionCoordinate connectionCoordinate() {
    return connectionCoordinate;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    this.catalog = catalog;
  }

  @Override
  public String getCatalog() throws SQLException {
    return catalog;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    this.schema = schema;
  }

  @Override
  public String getSchema() throws SQLException {
    return schema;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return sql;
  }


  @Override
  public Clob createClob() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw Exceptions.unsupported();
  }
}
