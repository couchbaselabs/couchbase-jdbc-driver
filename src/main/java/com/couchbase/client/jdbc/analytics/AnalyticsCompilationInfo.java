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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JsonObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class AnalyticsCompilationInfo {

  private static final String OPTIONAL_TYPE_SUFFIX = "?";

  private final List<String> columnNames = new ArrayList<>();
  private final List<String> columnTypes = new ArrayList<>();
  private final String statementCategory;
  private final List<Object> statementParameters;

  AnalyticsCompilationInfo(JsonObject signature, JsonObject plans) {
    if (plans.getArray("statementParameters") != null) {
      statementParameters = plans.getArray("statementParameters").toList();
    } else {
      statementParameters = Collections.emptyList();
    }
    statementCategory = plans.getString("statementCategory");

    for (Object name : signature.getArray("name")) {
      columnNames.add((String) name);
    }
    for (Object type : signature.getArray("type")) {
      columnTypes.add((String) type);
    }
  }

  public int parameterCount() {
    if (statementParameters == null) {
      return 0;
    }

    int paramPos = 0;
    for (Object param : statementParameters) {
      if (param instanceof Number) {
        paramPos = Math.max(paramPos, ((Number) param).intValue());
      } else {
        throw new CouchbaseException("Unsupported statementParameters type: " + param);
      }
    }
    return paramPos;
  }

  public List<AnalyticsColumn> columns() throws SQLException {
    int count = columnNames.size();
    List<AnalyticsColumn> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String columnName = columnNames.get(i);
      String typeName = columnTypes.get(i);
      boolean optional = false;
      if (typeName.endsWith(OPTIONAL_TYPE_SUFFIX)) {
        optional = true;
        typeName = typeName.substring(0, typeName.length() - OPTIONAL_TYPE_SUFFIX.length());
      }
      AnalyticsDataType columnType = AnalyticsDataType.findByTypeName(typeName);
      if (columnType == null) {
        throw new SQLException("Cannot infer result columns");
      }
      result.add(new AnalyticsColumn(columnName, columnType, optional));
    }
    return result;
  }

  @Override
  public String toString() {
    return "AnalyticsCompilationInfo{" +
      "columnNames=" + columnNames +
      ", columnTypes=" + columnTypes +
      ", statementCategory='" + statementCategory + '\'' +
      ", statementParameters=" + statementParameters +
      '}';
  }
}
