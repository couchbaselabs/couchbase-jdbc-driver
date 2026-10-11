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

package com.couchbase.client.jdbc.e2e;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * The couchbase-analytics-jdbc-driver flavor end to end against Couchbase Server with the analytics service.
 * <p>
 * This flavor bootstraps through the Java SDK, which reads node addresses out of the cluster config and would
 * otherwise be handed the node's container-internal address. {@link CouchbaseContainer} renames the node to
 * its internal IP and registers an "external" alternate address of {@code getHost()} plus the mapped ports;
 * because the JDBC URL seeds the SDK with that same host, the SDK selects the external network and every
 * connection (KV bootstrap, then analytics) goes through mapped ports. That holds whether the test JVM is on
 * the Docker host or in a sibling container: in the latter case {@code getHost()} is the docker gateway.
 * <p>
 * Image: {@code -De2e.server.image=...} (default {@code couchbase/server:enterprise-<server.version>}).
 */
class AnalyticsServerDockerE2EIT extends AbstractDockerE2EIT {

  private static final String IMAGE = System.getProperty("e2e.server.image", "couchbase/server:enterprise-7.6.9");
  private static final int CBAS_QUOTA_MB = Integer.getInteger("cbas.quota", 1200);

  private static CouchbaseContainer container;

  @BeforeAll
  static void startCluster() throws Exception {
    assumeDocker();
    container = new CouchbaseContainer(DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("couchbase/server"))
        .withEnabledServices(CouchbaseService.KV, CouchbaseService.ANALYTICS)
        .withServiceQuota(CouchbaseService.ANALYTICS, CBAS_QUOTA_MB)
        .withCredentials(USER, PASSWORD)
        .withStartupTimeout(Duration.ofMinutes(5))
        .withStartupAttempts(3);
    container.start();
    awaitAnalyticsQueries(container, Duration.ofMinutes(3));
  }

  @AfterAll
  static void stopCluster() {
    if (container != null) {
      container.stop();
    }
  }

  @Override
  protected String jdbcUrl() {
    // host:port seeds the SDK; a bare port is the KV (memcached) port, which the SDK bootstraps from.
    return "jdbc:couchbase:analytics://" + container.getHost() + ":" + container.getBootstrapCarrierDirectPort();
  }
}
