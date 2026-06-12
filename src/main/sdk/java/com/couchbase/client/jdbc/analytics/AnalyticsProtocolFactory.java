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

import com.couchbase.analytics.client.java.AnalyticsException;
import com.couchbase.analytics.client.java.AnalyticsTimeoutException;
import com.couchbase.analytics.client.java.InvalidCredentialException;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.util.Map;
import java.util.Properties;

import static com.couchbase.client.jdbc.ErrorUtils.authError;

/**
 * Creates the Analytics SDK-backed protocol ({@link AnalyticsSdkProtocol}).
 * <p>
 * This class is present only in the "sdk" build flavor. The "couchbase-analytics" flavor provides a
 * different implementation with the same fully-qualified name, so {@link AnalyticsDriver}
 * can delegate to it without referencing either protocol directly.
 */
final class AnalyticsProtocolFactory {

  private AnalyticsProtocolFactory() {
  }

  static void shutdown() {
    // No connection manager to shut down in the Analytics SDK flavor.
  }

  static ADBProtocolBase create(final Properties properties, final String hostname, final int port,
                                final Map<ADBDriverProperty, Object> map, final ADBDriverContext ctx)
      throws SQLException {
    try {
      return new AnalyticsSdkProtocol(properties, hostname, port, ctx, map);
    } catch (AnalyticsTimeoutException ex) {
      throw new SQLTimeoutException("Could not connect to the Cluster in the given connectTimeout interval.", ex);
    } catch (InvalidCredentialException ex) {
      throw authError(ex);
    } catch (AnalyticsException ex) {
      // Any other failure surfaced by the Analytics SDK while creating the protocol.
      throw new SQLException("Failed to connect to the Analytics cluster: " + ex.getMessage(), ex);
    } catch (IllegalArgumentException ex) {
      // Bad scanWait or scanConsistency connection property — surface as a SQL error so callers
      // that only catch SQLException still see a diagnostic message.
      throw new SQLNonTransientConnectionException(ex.getMessage(), "22023", ex);
    }
  }
}
