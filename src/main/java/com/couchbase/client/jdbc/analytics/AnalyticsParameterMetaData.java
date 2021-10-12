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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class AnalyticsParameterMetaData implements ParameterMetaData {

  private final AnalyticsPreparedStatement statement;
  private final int parameterCount;

  public AnalyticsParameterMetaData(AnalyticsPreparedStatement statement, int parameterCount) {
    this.statement = statement;
    this.parameterCount = parameterCount;
  }

  AnalyticsPreparedStatement statement() {
    return statement;
  }

  @Override
  public int getParameterCount() throws SQLException {
    return parameterCount;
  }

  @Override
  public int isNullable(int param) throws SQLException {
    return parameterNullable;
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    return false;
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int param) throws SQLException {
    return 0;
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    return Types.OTHER; // any
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    return "";
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    return Object.class.getName();
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    return parameterModeIn;
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
