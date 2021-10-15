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

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.env.PropertyLoader;
import com.couchbase.client.core.env.SystemPropertyPropertyLoader;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;

/**
 * The {@link ConnectionManager} encapsulates SDK access and allows performing SDK operations.
 *
 * Access only through the {@link #INSTANCE} and then grab a {@link #handle(ConnectionCoordinate)}.
 */
public class ConnectionManager {

  /**
   * The singleton instance that should be used for the manager.
   */
  public static ConnectionManager INSTANCE = new ConnectionManager();

  private final Map<String, Cluster> clusterCache = new ConcurrentHashMap<>();
  private volatile ClusterEnvironment environment;

  private ConnectionManager() {}

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

        environment = ClusterEnvironment
          .builder()
          .load((PropertyLoader<CoreEnvironment.Builder>) builder ->
            new SystemPropertyPropertyLoader(coordinate.properties()).load(builder))
          .loggerConfig(loggerConfig)
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
