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


import com.couchbase.client.jdbc.common.CommonResultSet;
import com.couchbase.client.jdbc.util.Exceptions;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;

public class AnalyticsResultSet extends CommonResultSet {

  private final AnalyticsStatement statement;
  private final int maxRows;

  AnalyticsResultSet(final AnalyticsResultSetMetaData metaData, final AnalyticsStatement statement, final int maxRows, final List<LinkedHashMap<String, Object>> rows) {
    super(metaData, rows);
    this.statement = statement;
    this.maxRows = maxRows;
  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void insertRow() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void updateRow() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw Exceptions.unsupported();
  }

}
