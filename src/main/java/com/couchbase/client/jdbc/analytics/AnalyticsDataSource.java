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
import org.apache.asterix.jdbc.core.ADBDriverProperty;

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

  private final AnalyticsDriver analyticsDriver;

  public static AnalyticsDataSource.Builder builder() {
    return new Builder();
  }

  private AnalyticsDataSource(Builder builder) {
    this.hostname = builder.hostname;
    this.properties = builder.properties;
    this.url = builder.url;
    this.catalog = builder.catalog;
    this.schema = builder.schema;

    this.analyticsDriver = new AnalyticsDriver(properties, "asterixdb:", 0);
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
    String url = "jdbc:asterixdb://"  + hostname + "/" + catalog;
    if (schema != null && !schema.isEmpty()) {
      url = url + "/" + schema;
    }

    Properties properties = new Properties();
    properties.setProperty(ADBDriverProperty.Common.USER.getPropertyName(), username);
    properties.setProperty(ADBDriverProperty.Common.PASSWORD.getPropertyName(), password);

    // TODO: proxy more properties

    return analyticsDriver.connect(url, properties);
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
