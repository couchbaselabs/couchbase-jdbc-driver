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

import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.jdbc.CouchbaseDriver;
import org.apache.asterix.jdbc.core.ADBDriverBase;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProductVersion;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

import java.net.URI;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Map;
import java.util.Properties;

/**
 * The {@link AnalyticsDriver} provides the main entry point into the Analytics through the {@link ADBDriverBase}.
 * <p>
 * For the I/O components, see the {@link AnalyticsProtocol}.
 */
public class AnalyticsDriver extends ADBDriverBase {

  private final Properties properties;

  public AnalyticsDriver(final Properties properties, final String driverScheme, final int defaultApiPort) {
    super(driverScheme, defaultApiPort);
    this.properties = properties;
  }

  @Override
  protected ADBProtocolBase createProtocol(final String hostname, final int port,
                                           final Map<ADBDriverProperty, Object> map, final ADBDriverContext ctx) throws SQLException {
    try {
      return new AnalyticsProtocol(properties, hostname, port, ctx, map);
    } catch (TimeoutException ex) {
      throw new SQLTimeoutException("Could not connect to the Cluster in the given connectTimeout interval.", ex);
    }
  }

  @Override
  protected Properties getURIParameters(final URI uri) {
    return new Properties(); // The actual properties are passed through the AnalyticsProtocol.
  }

  @Override
  protected ADBProductVersion getDriverVersion() {
    return new ADBProductVersion(
      CouchbaseDriver.DRIVER_NAME,
      CouchbaseDriver.DRIVER_VERSION.get(),
      CouchbaseDriver.DRIVER_MAJOR_VERSION.get(),
      CouchbaseDriver.DRIVER_MINOR_VERSION.get()
    );
  }

  @Override
  public ADBDriverContext getOrCreateDriverContext() {
    return super.getOrCreateDriverContext();
  }

}
