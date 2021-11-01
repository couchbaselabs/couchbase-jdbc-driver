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
import org.apache.asterix.jdbc.core.ADBDriverProperty;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The {@link AnalyticsDataSource} can be used to programmatically construct a JDBC driver which allows to interact
 * with Couchbase Analytics.
 */
public class AnalyticsDataSource implements DataSource {

  public static final String DEFAULT_CATALOG = "Default";

  private final String hostname;
  private final Properties properties;
  private final String catalog;
  private final String schema;
  private final AnalyticsDriver analyticsDriver;

  public static AnalyticsDataSource.Builder builder() {
    return new Builder();
  }

  private AnalyticsDataSource(Builder builder) {
    this.hostname = builder.hostname;
    this.properties = builder.properties;
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

    Properties adbProperties = new Properties();
    adbProperties.setProperty(ADBDriverProperty.Common.USER.getPropertyName(), username);
    adbProperties.setProperty(ADBDriverProperty.Common.PASSWORD.getPropertyName(), password);


    String dataverseMode = CouchbaseDriverProperty.CATALOG_DATAVERSE_MODE.get(properties);
    if ("catalogSchema".equals(dataverseMode)) {
      adbProperties.setProperty(ADBDriverProperty.Common.CATALOG_DATAVERSE_MODE.getPropertyName(), "2");
    }

    adbProperties.setProperty(
      ADBDriverProperty.Common.CATALOG_INCLUDES_SCHEMALESS.getPropertyName(),
      CouchbaseDriverProperty.CATALOG_INCLUDE_SCHEMALESS.get(properties)
    );

    adbProperties.setProperty(
      ADBDriverProperty.Common.MAX_WARNINGS.getPropertyName(),
      CouchbaseDriverProperty.MAX_WARNINGS.get(properties)
    );

    adbProperties.setProperty(
      ADBDriverProperty.Common.SQL_COMPAT_MODE.getPropertyName(),
      CouchbaseDriverProperty.SQL_COMPAT_MODE.get(properties)
    );

    String minDriverVersion = CouchbaseDriverProperty.MIN_DRIVER_VERSION.get(properties);
    if (minDriverVersion != null) {
      adbProperties.setProperty(
        ADBDriverProperty.Common.MIN_DRIVER_VERSION.getPropertyName(),
        minDriverVersion
      );
    }

    String minDatabaseVersion = CouchbaseDriverProperty.MIN_DATABASE_VERSION.get(properties);
    if (minDatabaseVersion != null) {
      adbProperties.setProperty(
        ADBDriverProperty.Common.MIN_DATABASE_VERSION.getPropertyName(),
        minDatabaseVersion
      );
    }

    return analyticsDriver.connect(url, adbProperties);
  }

  @Override
  public final boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }

  @Override
  public final <T> T unwrap(Class<T> iface) throws SQLException {
    if (!iface.isInstance(this)) {
      throw analyticsDriver.getOrCreateDriverContext().getErrorReporter().errorUnwrapTypeMismatch(iface);
    }
    return iface.cast(this);
  }

  @Override
  public PrintWriter getLogWriter() {
    return null; // We are not using a log writer.
  }

  @Override
  public void setLogWriter(final PrintWriter out) {
    // noop
  }

  @Override
  public void setLoginTimeout(final int seconds) {
    throw new UnsupportedOperationException("setLoginTimeout is not supported");
  }

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public Logger getParentLogger() {
    return CouchbaseDriver.PARENT_LOGGER;
  }

  /**
   * This Builder allows to customize the data source.
   */
  public static class Builder {

    private String hostname = CouchbaseDriver.DEFAULT_HOSTNAME;
    private String catalog = DEFAULT_CATALOG;
    private String schema = null;
    private Properties properties = new Properties();

    public Builder hostname(final String hostname) {
      this.hostname = hostname == null || hostname.isEmpty() ? CouchbaseDriver.DEFAULT_HOSTNAME : hostname;
      return this;
    }

    public Builder catalog(final String catalog) {
      this.catalog = catalog == null || catalog.isEmpty() ? DEFAULT_CATALOG : catalog;
      return this;
    }

    public Builder schema(final String schema) {
      this.schema = schema;
      return this;
    }

    public Builder properties(final Properties properties) {
      this.properties = properties == null ? new Properties() : properties;
      return this;
    }

    /**
     * Creates the {@link AnalyticsDataSource} from the custom properties.
     *
     * @return the created and ready-to-use data source.
     */
    public AnalyticsDataSource build() {
      return new AnalyticsDataSource(this);
    }
  }

}
