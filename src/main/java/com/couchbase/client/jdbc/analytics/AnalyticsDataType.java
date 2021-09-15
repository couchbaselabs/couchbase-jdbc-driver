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

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

enum AnalyticsDataType {
  TINYINT(1, "int8", JDBCType.TINYINT, Byte.class),
  SMALLINT(2, "int16", JDBCType.SMALLINT, Short.class),
  INTEGER(3, "int32", JDBCType.INTEGER, Integer.class),
  BIGINT(4, "int64", JDBCType.BIGINT, Long.class),

  FLOAT(11, "float", JDBCType.REAL, Float.class),
  DOUBLE(12, "double", JDBCType.DOUBLE, Double.class),
  STRING(13, "string", JDBCType.VARCHAR, String.class),
  MISSING(14, "missing", JDBCType.OTHER, Void.class), // don't report as JDBCType.NULL
  BOOLEAN(15, "boolean", JDBCType.BOOLEAN, Boolean.class),
  DATETIME(16, "datetime", JDBCType.TIMESTAMP, java.sql.Timestamp.class),
  DATE(17, "date", JDBCType.DATE, java.sql.Date.class),
  TIME(18, "time", JDBCType.TIME, java.sql.Time.class),
  DURATION(19, "duration", JDBCType.OTHER, String.class),

  ARRAY(22, "array", JDBCType.OTHER, List.class),
  MULTISET(23, "multiset", JDBCType.OTHER, List.class),
  OBJECT(24, "object", JDBCType.OTHER, Map.class),

  ANY(29, "any", JDBCType.OTHER, String.class),

  YEARMONTHDURATION(36, "year-month-duration", JDBCType.OTHER, java.time.Period.class),
  DAYTIMEDURATION(37, "day-time-duration", JDBCType.OTHER, java.time.Duration.class),
  UUID(38, "uuid", JDBCType.OTHER, java.util.UUID.class),

  NULL(41, "null", JDBCType.NULL, Void.class);

  private static final AnalyticsDataType[] BY_TYPE_TAG;

  private static final Map<String, AnalyticsDataType> BY_TYPE_NAME;

  private final byte typeTag;

  private final String typeName;

  private final JDBCType jdbcType;

  private final Class<?> javaClass;

  AnalyticsDataType(int typeTag, String typeName, JDBCType jdbcType, Class<?> javaClass) {
    this.typeTag = (byte) typeTag;
    this.typeName = Objects.requireNonNull(typeName);
    this.jdbcType = Objects.requireNonNull(jdbcType);
    this.javaClass = Objects.requireNonNull(javaClass);
  }

  byte getTypeTag() {
    return typeTag;
  }

  String getTypeName() {
    return typeName;
  }

  JDBCType getJdbcType() {
    return jdbcType;
  }

  Class<?> getJavaClass() {
    return javaClass;
  }

  @Override
  public String toString() {
    return getTypeName();
  }

  boolean isDerived() {
    return this == OBJECT || isList();
  }

  boolean isList() {
    return this == ARRAY || this == MULTISET;
  }

  boolean isNullOrMissing() {
    return this == NULL || this == MISSING;
  }

  static {
    AnalyticsDataType[] allTypes = AnalyticsDataType.values();
    AnalyticsDataType[] byTypeTag = new AnalyticsDataType[findMaxTypeTag(allTypes) + 1];
    Map<String, AnalyticsDataType> byTypeName = new HashMap<>();
    for (AnalyticsDataType t : allTypes) {
      byTypeTag[t.typeTag] = t;
      byTypeName.put(t.typeName, t);
    }
    BY_TYPE_TAG = byTypeTag;
    BY_TYPE_NAME = byTypeName;
  }

  public static AnalyticsDataType findByTypeTag(byte typeTag) {
    return typeTag >= 0 && typeTag < BY_TYPE_TAG.length ? BY_TYPE_TAG[typeTag] : null;
  }

  public static AnalyticsDataType findByTypeName(String typeName) {
    return BY_TYPE_NAME.get(typeName);
  }

  private static int findMaxTypeTag(AnalyticsDataType[] allTypes) {
    int maxTypeTag = 0;
    for (AnalyticsDataType type : allTypes) {
      if (type.typeTag < 0) {
        throw new IllegalStateException(type.getTypeName());
      }
      maxTypeTag = Math.max(type.typeTag, maxTypeTag);
    }
    return maxTypeTag;
  }

  static String getDerivedRecordName(AnalyticsDataType type) {
    switch (type) {
      case OBJECT:
        return "Record";
      case ARRAY:
        return "OrderedList";
      case MULTISET:
        return "UnorderedList";
      default:
        throw new IllegalArgumentException(String.valueOf(type));
    }
  }
}
