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

import com.couchbase.client.jdbc.CouchbaseDriver;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;
import com.couchbase.client.jdbc.common.CommonDataSource;
import com.couchbase.client.jdbc.sdk.ConnectionCoordinate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class AnalyticsDataSource extends CommonDataSource {

  public static final String DEFAULT_CATALOG = "Default";

  private final String hostname;
  private final Properties properties;
  private final String url;
  private final String catalog;
  private final String schema;

  public static AnalyticsDataSource.Builder builder() {
    return new Builder();
  }

  private AnalyticsDataSource(Builder builder) {
    this.hostname = builder.hostname;
    this.properties = builder.properties;
    this.url = builder.url;
    this.catalog = builder.catalog;
    this.schema = builder.schema;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(
      properties.getProperty(CouchbaseDriverProperty.USER.getName()),
      properties.getProperty(CouchbaseDriverProperty.PASSWORD.getName())
    );
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return new AnalyticsConnection(
      ConnectionCoordinate.create(buildUrl(), hostname, username, password, properties),
      catalog,
      schema,
      Boolean.parseBoolean(CouchbaseDriverProperty.CATALOG_INCLUDE_SCHEMALESS.get(properties)),
      AnalyticsCatalogDataverseMode.fromString(CouchbaseDriverProperty.CATALOG_DATAVERSE_MODE.get(properties))
    );
  }

  /**
   * Return the URL or build one if not provided by the caller.
   */
  String buildUrl() {
    return url == null ? CouchbaseDriver.ANALYTICS_URL_PREFIX + "://" + hostname : url;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(getClass());
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  public static class Builder {

    private String url = null;
    private String hostname = CouchbaseDriver.DEFAULT_HOSTNAME;
    private String catalog = DEFAULT_CATALOG;
    private String schema = null;
    private Properties properties = new Properties();

    public Builder url(String url) {
      this.url = url;
      return this;
    }

    public Builder hostname(String hostname) {
      this.hostname = hostname == null || hostname.isEmpty() ? CouchbaseDriver.DEFAULT_HOSTNAME : hostname;
      return this;
    }

    public Builder catalog(String catalog) {
      this.catalog = catalog == null || catalog.isEmpty() ? DEFAULT_CATALOG : catalog;
      return this;
    }

    public Builder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder properties(Properties properties) {
      this.properties = properties == null ? new Properties() : properties;
      return this;
    }

    public AnalyticsDataSource build() {
      return new AnalyticsDataSource(this);
    }
  }

}
