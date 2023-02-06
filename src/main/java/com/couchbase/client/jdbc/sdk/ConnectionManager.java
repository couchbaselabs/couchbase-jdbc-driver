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

package com.couchbase.client.jdbc.sdk;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.env.PropertyLoader;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.SystemPropertyPropertyLoader;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryAction;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static java.util.Objects.isNull;

/**
 * The {@link ConnectionManager} encapsulates SDK access and allows performing SDK operations.
 *
 * Access only through the {@link #INSTANCE} and then grab a {@link #handle(ConnectionCoordinate)}.
 */
public class ConnectionManager {

  private static final Logger LOGGER = Logger.getLogger("ConnectionManager");

  /**
   * The singleton instance that should be used for the manager.
   */
  public static ConnectionManager INSTANCE = new ConnectionManager();

  private final Map<ConnectionCoordinate, Cluster> clusterCache = new ConcurrentHashMap<>();
  private final Map<ConnectionCoordinate, Long> openHandles = new ConcurrentHashMap<>();

  private ConnectionManager() {
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    } catch (Exception ex) {
      LOGGER.log(Level.WARNING, "Could not initialize shutdown hook", ex);
    }
  }

  /**
   * This method can be used to proactively shutdown the SDK and all its resources.
   */
  public synchronized void shutdown() {
    LOGGER.fine("Shutting down connected SDK resources.");

    clusterCache.forEach((s, cluster) -> {
      cluster.disconnect();
      if (cluster.environment() != null) {
        cluster.environment().shutdown();
      }
    });
    clusterCache.clear();

    LOGGER.finest("Completed shutting down connected SDK resources.");
  }

  /**
   * Returns a new thread safe handle for the given connection coordinate.
   *
   * @param coordinate the coordinate.
   * @return the handle.
   */
  public ConnectionHandle handle(final ConnectionCoordinate coordinate) {
    return new ConnectionHandle(clusterForCoordinate(coordinate), coordinate);
  }

  private Cluster clusterForCoordinate(final ConnectionCoordinate coordinate) {
    synchronized (this) {

      final ClusterEnvironment environment;
      if (!clusterCache.containsKey(coordinate)) {
        environment = generateEnvironment(coordinate);
      } else {
        environment = null;
      }

      long newHandleCount = openHandles.compute(coordinate, (k, v) -> {
        if (v == null) {
          return 1L;
        } else {
          return v + 1;
        }
      });

      LOGGER.fine("Incrementing Handle Count to " + newHandleCount + " for Coordinate " + coordinate);

      return clusterCache.computeIfAbsent(
        coordinate,
        s -> {
          Cluster c = Cluster.connect(
            coordinate.connectionString(),
            clusterOptions(coordinate.authenticator()).environment(environment)
          );

          maybeWaitUntilReady(coordinate, c);

          return c;
        }
      );
    }
  }

  private ClusterEnvironment generateEnvironment(final ConnectionCoordinate coordinate) {
    // This logger config makes sure the SDK also uses java.util.Logging.
    LoggerConfig.Builder loggerConfig = LoggerConfig
            .builder()
            .disableSlf4J(true)
            .fallbackToConsole(false);

    SecurityConfig.Builder securityConfig = SecurityConfig
            .builder();

    if (Boolean.parseBoolean(CouchbaseDriverProperty.SSL.get(coordinate.properties()))) {
      securityConfig = securityConfig.enableTls(true);

      if ("no-verify".equals(CouchbaseDriverProperty.SSL_MODE.get(coordinate.properties()))) {
        securityConfig = securityConfig.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
      } else {
        if ("verify-ca".equals(CouchbaseDriverProperty.SSL_MODE.get(coordinate.properties()))) {
          securityConfig = securityConfig.enableHostnameVerification(false);
        }

        String certPath = CouchbaseDriverProperty.SSL_CERT_PATH.get(coordinate.properties());
        if (!isNullOrEmpty(certPath)) {
          securityConfig.trustCertificate(Paths.get(certPath));
        }

        String keyStorePath = CouchbaseDriverProperty.SSL_KEYSTORE_PATH.get(coordinate.properties());
        if (!isNullOrEmpty(keyStorePath)) {
          if (!isNullOrEmpty(certPath)) {
            throw new IllegalArgumentException("Either trust certificates or a trust store can be provided, but not both");
          }
          String keyStorePassword = CouchbaseDriverProperty.SSL_KEYSTORE_PASSWORD.get(coordinate.properties());
          if (isNull(keyStorePassword)) {
            throw new IllegalArgumentException("If a keystore is provided, the password also needs to be provided");
          }
          securityConfig.trustStore(
                  Paths.get(keyStorePath),
                  keyStorePassword,
                  Optional.empty()
          );
        }
      }
    }

    return ClusterEnvironment
            .builder()
            .load((PropertyLoader<CoreEnvironment.Builder>) builder ->
                    new SystemPropertyPropertyLoader(coordinate.properties()).load(builder))
            .loggerConfig(loggerConfig)
            .securityConfig(securityConfig)
            .retryStrategy(new InterceptingRetryStrategy())
            .build();
  }

  /**
   * Helper method to perform the wait until ready logic if needed.
   * <p>
   * In order to spot auth issues earlier, the actual wait until ready time is 500ms, but then we'll try up to the
   * given connect timeout (if any) to give it the maximum amount of possible wait time. If we detect an auth
   * failure, we bail out early.
   *
   * @param coordinate the coordinate.
   * @param c the reference cluster.
   */
  private void maybeWaitUntilReady(final ConnectionCoordinate coordinate, final Cluster c) {
    Duration connectTimeout = coordinate.connectTimeout();
    if (connectTimeout == null || connectTimeout.isZero()) {
      LOGGER.fine("No connectTimeout set, so not performing waitUntilReady");
      return;
    }

    LOGGER.fine("Applying cumulative WaitUntilReady timeout (connectTimeout) of " + connectTimeout);

    Duration waitUntilReadyTimeout = Duration.ofMillis(500);
    Duration totalDurationSpent = Duration.ZERO;

    while (true) {
      try {
        c.waitUntilReady(waitUntilReadyTimeout);
        break;
      } catch (Exception x) {
        totalDurationSpent = totalDurationSpent.plus(waitUntilReadyTimeout);

        DiagnosticsResult diagnostics = c.diagnostics();
        if (hasAuthFailure(diagnostics)) {
          c.disconnect();
          if (c.environment() != null) {
            c.environment().shutdown();
          }
          decrementHandleCount(coordinate);
          throw new AuthenticationFailureException("Authentication/authorization error - please verify credentials.", null, x);
        } else if (totalDurationSpent.compareTo(connectTimeout) >= 0) {
          c.disconnect();
          if (c.environment() != null) {
            c.environment().shutdown();
          }
          decrementHandleCount(coordinate);
          throw x;
        }
      }
    }
  }

  /**
   * Check if the diagnostic result contains an auth failure.
   *
   * @param result the result to check.
   * @return true if it does.
   */
  private static boolean hasAuthFailure(final DiagnosticsResult result) {
    if (result == null) {
      return false;
    }

    for (List<EndpointDiagnostics> endpoints : result.endpoints().values()) {
      for (EndpointDiagnostics diagnostics : endpoints) {
        if (hasAuthFailure(diagnostics.lastConnectAttemptFailure())) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Check if the throwable itself is present and an auth error.
   *
   * @param lastError the throwable to check.
   * @return true if it is present and an auth error.
   */
  static boolean hasAuthFailure(final Optional<Throwable> lastError) {
    return lastError.isPresent() && lastError.get() instanceof AuthenticationFailureException;
  }

  /**
   * Closes the connection associated with this coordinate if the reference count reaches zero.
   *
   * @param coordinate the coordinate to close the connection for.
   */
  public synchronized void maybeClose(final ConnectionCoordinate coordinate) {
    long newHandleCount = decrementHandleCount(coordinate);

    if (newHandleCount <= 0) {
      LOGGER.fine("Coordinate " + coordinate + " reached count 0, disconnecting Cluster instance.");

      Cluster toRemove = clusterCache.remove(coordinate);
      if(toRemove != null) {
        toRemove.disconnect();
      }
    }
  }

  private long decrementHandleCount(final ConnectionCoordinate coordinate) {
    long newHandleCount = openHandles.compute(coordinate, (k, v) -> {
      if (v == null) {
        throw new IllegalStateException("No handle present for Coordinate, this should have not happened! " + coordinate);
      } else {
        return v - 1;
      }
    });

    LOGGER.fine("Decrementing Handle Count to " + newHandleCount + " for Coordinate " + coordinate);
    return newHandleCount;
  }

  /**
   * This retry strategy checks on a retry attempt if the diagnostics report auth failures - and if so short-circuits
   * the request instead of continuing until timeout.
   */
  static class InterceptingRetryStrategy extends BestEffortRetryStrategy {
    @Override
    public CompletableFuture<RetryAction> shouldRetry(final Request<? extends Response> request,
                                                      final RetryReason reason) {
      boolean isAuthError = request
        .context()
        .core()
        .diagnostics()
        .anyMatch(ed -> hasAuthFailure(ed.lastConnectAttemptFailure()));

      if (isAuthError) {
        return CompletableFuture.completedFuture(
          RetryAction.noRetry(t -> new AuthenticationFailureException("Authentication failure detected", null, t))
        );
      } else {
        return super.shouldRetry(request, reason);
      }
    }
  }

}
