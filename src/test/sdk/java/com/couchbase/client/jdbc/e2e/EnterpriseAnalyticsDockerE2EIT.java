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

/**
 * The couchbase-operational-insights-jdbc-driver flavor end to end against Enterprise Analytics, which the
 * Analytics SDK underneath this flavor also supports. Image: {@code -De2e.ea.image=...}.
 */
class EnterpriseAnalyticsDockerE2EIT extends AbstractAnalyticsNodeDockerE2EIT {

  private static final String IMAGE = System.getProperty("e2e.ea.image", "couchbase/enterprise-analytics:2.2.1");

  private static Cluster cluster;

  @BeforeAll
  static void start() throws Exception {
    cluster = startCluster(IMAGE);
  }

  @AfterAll
  static void stop() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Override
  protected String jdbcUrl() {
    return cluster.jdbcUrl();
  }
}
