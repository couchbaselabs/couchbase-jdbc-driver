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

import com.couchbase.client.jdbc.util.Exceptions;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.Validators.notNull;

public class AnalyticsResultSetMetaData implements ResultSetMetaData {

  private final AnalyticsStatement statement;
  private final List<AnalyticsColumn> columns;
  private final Map<String, Integer> indexByName;

  AnalyticsResultSetMetaData(AnalyticsStatement statement, List<AnalyticsColumn> columns) {
    this.statement = notNull(statement, "Statement");
    this.columns = columns != null ? columns : Collections.emptyList();
    this.indexByName = createIndexByName(this.columns);
  }

  private static Map<String, Integer> createIndexByName(List<AnalyticsColumn> columns) {
    int n = columns.size();
    switch (n) {
      case 0:
        return Collections.emptyMap();
      case 1:
        return Collections.singletonMap(columns.get(0).name(), 0);
      default:
        Map<String, Integer> m = new HashMap<>();
        for (int i = 0; i < n; i++) {
          m.put(columns.get(i).name(), i);
        }
        return m;
    }
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columns.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return columnByNumber(column).optional() ? columnNullable : columnNoNulls;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 1; // TODO: based on size
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return columnByNumber(column).name();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return "";
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0; // TODO: based on type
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return "";
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "";
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return columnByNumber(column).dataType().getJdbcType().getVendorTypeNumber();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return columnByNumber(column).dataType().getTypeName();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return columnByNumber(column).dataType().getJavaClass().getName();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  private AnalyticsColumn columnByNumber(int columnNumber) throws SQLException {
    return columnByIndex(toColumnIndex(columnNumber));
  }

  private int toColumnIndex(int columnNumber) throws SQLException {
    boolean ok = 0 < columnNumber && columnNumber <= columns.size();
    if (!ok) {
      throw Exceptions.unsupported();
    }
    return columnNumber - 1;
  }

  private AnalyticsColumn columnByIndex(int idx) {
    return columns.get(idx);
  }

  int findColumnIndexByName(String columnName) {
    Integer idx = indexByName.get(columnName);
    return idx != null ? idx : -1;
  }

  AnalyticsStatement statement() {
    return statement;
  }

  @Override
  public String toString() {
    return "AnalyticsResultSetMetaData{" +
      "statement=" + statement +
      ", columns=" + columns +
      ", indexByName=" + indexByName +
      '}';
  }
}
