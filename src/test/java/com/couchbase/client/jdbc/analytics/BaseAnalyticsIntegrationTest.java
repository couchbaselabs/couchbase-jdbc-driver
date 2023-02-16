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

import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.java.Cluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.DriverManager;

abstract class BaseAnalyticsIntegrationTest extends DockerIntegrationTestBase {

  static String url() {
    return "jdbc:couchbase:analytics://" + hostname();
  }

  static String hostname() {
    return ConnectionString.create(container.getConnectionString())
            .hosts().stream().findFirst()
            .map(it -> it.hostname() + ":" + it.port())
            .orElseThrow(() -> new IllegalArgumentException(
                    "Connection string must have at least one host"
            ));
  }

  static String username() {
    return SERVER_USER;
  }

  static String password() {
    return SERVER_PASSWORD;
  }

}
