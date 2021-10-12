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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

public abstract class CommonResultSet implements ResultSet {

  public static final int TYPE = TYPE_FORWARD_ONLY;
  public static final int CONCURRENCY = CONCUR_READ_ONLY;
  public static final int HOLDABILITY = HOLD_CURSORS_OVER_COMMIT;
  public static final int FETCH_DIRECTION = FETCH_FORWARD;

  private final List<LinkedHashMap<String, Object>> rows;
  private final Iterator<LinkedHashMap<String, Object>> rowsIterator;
  private final ResultSetMetaData metaData;

  private volatile LinkedHashMap<String, Object> currentRow = null;
  private int currentRowIndex = 0;
  private List<String> labelIndexes = null;

  protected CommonResultSet(final ResultSetMetaData metaData, final List<LinkedHashMap<String, Object>> rows) {
    this.rows = rows;
    this.rowsIterator = rows.iterator();
    this.metaData = metaData;
  }

  private String labelForIndex(int columnIndex) {
    return labelIndexes.get(columnIndex - 1);
  }

  private <T> T wrapDecode(String columnLabel, Function<String, T> function) throws SQLException {
    try {
      return function.apply(columnLabel);
    } catch (Exception ex) {
      throw new SQLException("Could not decode column with label: " + columnLabel, ex);
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metaData;
  }

  @Override
  public synchronized boolean next() throws SQLException {
    try {
      currentRow = rowsIterator.next();
      currentRowIndex++;
      if (labelIndexes == null) {
        labelIndexes = new ArrayList<>(currentRow.keySet());
      }
      return true;
    } catch (NoSuchElementException ex) {
      currentRow = null;
      return false;
    }
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return getString(labelForIndex(columnIndex));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return getBoolean(labelForIndex(columnIndex));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return getByte(labelForIndex(columnIndex));
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return getShort(labelForIndex(columnIndex));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return getInt(labelForIndex(columnIndex));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return getLong(labelForIndex(columnIndex));
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return getFloat(labelForIndex(columnIndex));
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return getDouble(labelForIndex(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return getBigDecimal(labelForIndex(columnIndex), scale);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return getBytes(labelForIndex(columnIndex));
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return getDate(labelForIndex(columnIndex));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return getTime(labelForIndex(columnIndex));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getTimestamp(labelForIndex(columnIndex));
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return getAsciiStream(labelForIndex(columnIndex));
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return getUnicodeStream(labelForIndex(columnIndex));
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return getBinaryStream(labelForIndex(columnIndex));
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (String) currentRow.get(label));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (boolean) currentRow.get(label));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (byte) currentRow.get(label));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (short) currentRow.get(label));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (int) currentRow.get(label));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (long) currentRow.get(label));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (float) currentRow.get(label));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return wrapDecode(columnLabel, (label) -> (double) currentRow.get(label));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return wrapDecode(columnLabel, (label) -> {
      Object val = currentRow.get(label);
      if (val instanceof String) {
        return new BigDecimal((String) val);
      } else if (val instanceof Double) {
        return new BigDecimal((Double) val);
      } else if (val instanceof Long) {
        return new BigDecimal((Long) val);
      } else if (val instanceof Integer) {
        return new BigDecimal((Integer) val);
      } else {
        throw new IllegalStateException("Unexpected type/value for column with label: " + columnLabel + " - " + val);
      }
    });
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    // TODO
    return new byte[0];
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public String getCursorName() throws SQLException {
    throw Exceptions.unsupported();
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getObject(labelForIndex(columnIndex));
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return getCharacterStream(labelForIndex(columnIndex));
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getBigDecimal(labelForIndex(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public synchronized boolean isBeforeFirst() throws SQLException {
    return currentRowIndex == 0;
  }

  @Override
  public synchronized boolean isAfterLast() throws SQLException {
    return currentRowIndex > 0 && currentRow == null;
  }

  @Override
  public synchronized boolean isFirst() throws SQLException {
    return currentRowIndex == 1;
  }

  @Override
  public synchronized boolean isLast() throws SQLException {
    return currentRowIndex == rows.size();
  }

  @Override
  public void beforeFirst() throws SQLException {
// TODO
  }

  @Override
  public void afterLast() throws SQLException {
// TODO
  }

  @Override
  public boolean first() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    // TODO
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    // TODO
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
// TODO
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
// TODO
  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    return TYPE;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return CONCURRENCY;
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
// TODO
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
// TODO
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
// TODO
  }

  @Override
  public int getHoldability() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {

// TODO
return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    // TODO
    return null;
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
  public String toString() {
    return "CommonResultSet{" +
      "rows=" + rows +
      ", metaData=" + metaData +
      ", currentRow=" + currentRow +
      '}';
  }
}
