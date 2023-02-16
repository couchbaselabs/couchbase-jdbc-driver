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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AnalyticsConnectionIntegrationTest extends BaseAnalyticsIntegrationTest {

  @BeforeAll
  static void setup() throws Exception {
    startContainer(AnalyticsConnectionIntegrationTest.class);
  }

  @AfterAll
  static void teardown() throws Exception {
    stopContainer();
  }

  @Test
  void usesDefaultConnectionProperties() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:couchbase:analytics://" + hostname(), username(), password());
    assertEquals("Default", connection.getCatalog());
    assertNull(connection.getSchema());
  }

  @Test
  void allowsSettingCatalog() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:couchbase:analytics://" + hostname() + "/foo", username(), password());
    assertEquals("foo", connection.getCatalog());
    assertNull(connection.getSchema());
  }

  @Test
  void allowsSettingCatalogAndSchema() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:couchbase:analytics://" + hostname() + "/foo/bar", username(), password());
    assertEquals("foo/bar", connection.getCatalog());
    assertNull(connection.getSchema());
  }

  @Test
  void appliesCustomCatalogProperty() throws Exception {
    String args = "?catalogDataverseMode=catalogSchema";
    Connection connection = DriverManager.getConnection("jdbc:couchbase:analytics://" + hostname() + "/foo/bar" + args, username(), password());
    assertEquals("foo", connection.getCatalog());
    assertEquals("bar", connection.getSchema());

    connection = DriverManager.getConnection("jdbc:couchbase:analytics://" + hostname() + "/foo" + args, username(), password());
    assertEquals("foo", connection.getCatalog());
    assertNull(connection.getSchema());
  }

  @Test
  void failsWithUnknownNamespace() {
    assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:foo:analytics://" + hostname(), username(), password()));
    assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:couchbase:bar://" + hostname(), username(), password()));
  }

  @Test
  void supportsAlternativeNamespace() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:cb:analytics://" + hostname(), username(), password());
    assertEquals("Default", connection.getCatalog());
    assertNull(connection.getSchema());
  }

}
