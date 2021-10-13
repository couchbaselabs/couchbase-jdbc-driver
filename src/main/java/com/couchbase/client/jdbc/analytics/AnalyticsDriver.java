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

import org.apache.asterix.jdbc.core.ADBDriverBase;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class AnalyticsDriver extends ADBDriverBase {

  private final Properties properties;

  public AnalyticsDriver(Properties properties, String driverScheme, int defaultApiPort) {
    super(driverScheme, defaultApiPort);

    this.properties = properties;
  }

  @Override
  protected ADBProtocolBase createProtocol(String hostname, int port, Map<ADBDriverProperty, Object> map,
                                           ADBDriverContext ctx) throws SQLException {
    return new AnalyticsProtocol(properties, hostname, ctx, map);
  }

}
