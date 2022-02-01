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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.env.PropertyLoader;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.SystemPropertyPropertyLoader;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
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

  private final Map<String, Cluster> clusterCache = new ConcurrentHashMap<>();
  private final Map<String, Long> openHandles = new ConcurrentHashMap<>();

  private volatile ClusterEnvironment environment;

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

    clusterCache.forEach((s, cluster) -> cluster.disconnect());
    clusterCache.clear();
    if (environment != null) {
      environment.shutdown();
      environment = null;
    }

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
      if (environment == null) {
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

        environment = ClusterEnvironment
          .builder()
          .load((PropertyLoader<CoreEnvironment.Builder>) builder ->
            new SystemPropertyPropertyLoader(coordinate.properties()).load(builder))
          .loggerConfig(loggerConfig)
          .securityConfig(securityConfig)
          .build();
      }

      long newHandleCount = openHandles.compute(coordinate.connectionString(), (k, v) -> {
        if (v == null) {
          return 1L;
        } else {
          return v + 1;
        }
      });

      LOGGER.fine("Incrementing Handle Count to " + newHandleCount + " for Coordinate " + coordinate);
    }

    return clusterCache.computeIfAbsent(
      coordinate.connectionString(),
      s -> {
        Cluster c = Cluster.connect(
          coordinate.connectionString(),
          clusterOptions(coordinate.authenticator()).environment(environment)
        );

        String connectTimeout = CouchbaseDriverProperty.CONNECT_TIMEOUT.get(coordinate.properties());
        if (connectTimeout != null && !connectTimeout.isEmpty()) {
          LOGGER.fine("Applying WaitUntilReady timeout (connectTimeout) of " + connectTimeout);
          c.waitUntilReady(Golang.parseDuration(connectTimeout));
        } else {
          LOGGER.fine("No connectTimeout set, so not performing waitUntilReady");
        }

        return c;
      }
    );
  }

  /**
   * Closes the connection associated with this coordinate if the reference count reaches zero.
   *
   * @param coordinate the coordinate to close the connection for.
   */
  public synchronized void maybeClose(final ConnectionCoordinate coordinate) {
    long newHandleCount = openHandles.compute(coordinate.connectionString(), (k, v) -> {
      if (v == null) {
        throw new IllegalStateException("No handle present for Coordinate, this should have not happened! " + coordinate);
      } else {
        return v - 1;
      }
    });

    LOGGER.fine("Decrementing Handle Count to " + newHandleCount + " for Coordinate " + coordinate);

    if (newHandleCount <= 0) {
      LOGGER.fine("Coordinate " + coordinate + " reached count 0, disconnecting Cluster instance.");

      Cluster toRemove = clusterCache.remove(coordinate.connectionString());
      if(toRemove != null) {
        toRemove.disconnect();
      }
    }
  }

}
