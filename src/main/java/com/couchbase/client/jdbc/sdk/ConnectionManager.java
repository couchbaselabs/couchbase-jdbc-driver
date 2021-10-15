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
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.jdbc.CouchbaseDriverProperty;

import java.nio.file.Paths;
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
    return new ConnectionHandle(clusterForCoordinate(coordinate));
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
    }

    return clusterCache.computeIfAbsent(
      coordinate.connectionString(),
      s -> Cluster.connect(
        coordinate.connectionString(),
        clusterOptions(coordinate.authenticator()).environment(environment)
      )
    );
  }

}
