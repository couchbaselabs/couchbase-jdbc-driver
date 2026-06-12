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
import org.apache.asterix.jdbc.core.ADBDriverBase;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProductVersion;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

import java.net.URI;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * The {@link AnalyticsDriver} provides the main entry point into the Analytics through the {@link ADBDriverBase}.
 * <p>
 * The concrete protocol implementation is selected at build time: the "sdk" and "capella-analytics"
 * build flavors each supply their own {@code AnalyticsProtocolFactory}, so this class never
 * references a specific protocol or its dependencies directly.
 */
public class AnalyticsDriver extends ADBDriverBase {

  private final Properties properties;

  /**
   * Creates a new {@link AnalyticsDriver} instance.
   *
   * @param properties the list of properties to use.
   * @param driverScheme the driver scheme to use.
   * @param defaultApiPort the default API port to use.
   */
  public AnalyticsDriver(final Properties properties, final String driverScheme, final int defaultApiPort) {
    super(driverScheme, defaultApiPort);
    this.properties = properties;
  }

  @Override
  protected ADBProtocolBase createProtocol(final String hostname, final int port,
                                           final Map<ADBDriverProperty, Object> map, final ADBDriverContext ctx) throws SQLException {
    // The protocol implementation is chosen at build time. Each build flavor supplies its own
    // AnalyticsProtocolFactory (same FQN), which instantiates its protocol and maps any
    // flavor-specific connection exceptions to SQLException.
    return AnalyticsProtocolFactory.create(properties, hostname, port, map, ctx);
  }

  @Override
  protected Properties getURIParameters(final URI uri) {
    return new Properties(); // The actual properties are passed through the AnalyticsProtocolFactory.
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

  public static void shutdown() {
    AnalyticsProtocolFactory.shutdown();
  }

}
