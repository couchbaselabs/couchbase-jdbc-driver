/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.jdbc.sdk.ConnectionManager;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Map;
import java.util.Properties;

import static com.couchbase.client.jdbc.ErrorUtils.authError;

/**
 * Creates the couchbase-analytics core-SDK-backed protocol ({@link AnalyticsProtocol}).
 * <p>
 * This class is present only in the "couchbase-analytics" build flavor. The "sdk" flavor provides
 * a different implementation with the same fully-qualified name, so {@link AnalyticsDriver} can
 * delegate to it without referencing either protocol directly.
 * <p>
 * Note: the legacy core SDK surfaces fewer typed exceptions than the Analytics SDK, so this factory
 * catches only {@code TimeoutException} and {@code AuthenticationFailureException} (not a broader
 * {@code AnalyticsException} hierarchy). This is intentional, not an oversight.
 */
final class AnalyticsProtocolFactory {

  private AnalyticsProtocolFactory() {
  }

  static void shutdown() {
    ConnectionManager.INSTANCE.shutdown();
  }

  static ADBProtocolBase create(final Properties properties, final String hostname, final int port,
                                final Map<ADBDriverProperty, Object> map, final ADBDriverContext ctx)
      throws SQLException {
    try {
      return new AnalyticsProtocol(properties, hostname, port, ctx, map);
    } catch (TimeoutException ex) {
      throw new SQLTimeoutException("Could not connect to the Cluster in the given connectTimeout interval.", ex);
    } catch (AuthenticationFailureException ex) {
      throw authError(ex);
    }
  }
}
