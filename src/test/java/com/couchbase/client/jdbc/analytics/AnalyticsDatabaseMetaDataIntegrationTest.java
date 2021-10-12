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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnalyticsDatabaseMetaDataIntegrationTest extends BaseAnalyticsIntegrationTest {

  private static Connection connection;
  private static DatabaseMetaData metaData;

  @BeforeAll
  static void setup() throws Exception {
    connection = DriverManager.getConnection(url(), username(), password());
    metaData = connection.getMetaData();
  }

  @AfterAll
  static void teardown() throws Exception {
    connection.close();
  }

  @Test
  void returnsConnection() throws Exception {
    assertEquals(connection, metaData.getConnection());
  }

  @Test
  void returnsProductName() throws Exception {
    assertEquals("Couchbase Server", metaData.getDatabaseProductName());
  }

  @Test
  void returnsUserName() throws Exception {
    assertEquals(username(), metaData.getUserName());
  }

  @Test
  void loadsCatalogs() throws Exception {
    ResultSet resultSet = metaData.getCatalogs();

    Set<String> foundCatalogs = new HashSet<>();
    while(resultSet.next()) {
      foundCatalogs.add(resultSet.getString("TABLE_CAT"));
    }

    assertTrue(foundCatalogs.contains("Default"));
    assertTrue(foundCatalogs.contains("Metadata"));
  }

  @Test
  void loadsDatabaseVersion() throws Exception {
    assertNotNull(metaData.getDatabaseProductVersion());
    assertTrue(metaData.getDatabaseMajorVersion() > 0);
    assertTrue(metaData.getDatabaseMinorVersion() >= 0);
  }

  @Test
  void returnsDriverVersion() throws Exception {
    assertNotNull(metaData.getDriverVersion());
  }

  @Test
  void loadsTables() throws Exception {
    ResultSet resultSet = metaData.getTables(null, null, null, null);

    int numEntries = 0;
    while (resultSet.next()) {
      assertNotNull(resultSet.getString("TABLE_CAT"));
      numEntries++;
    }
    assertTrue(numEntries > 0);
  }

  @Test
  void loadsSchemas() throws Exception {
    ResultSet resultSet = metaData.getSchemas();

    int numEntries = 0;
    while (resultSet.next()) {
      assertNotNull(resultSet.getString("TABLE_CATALOG"));
      numEntries++;
    }
    assertTrue(numEntries > 0);
  }

  @Test
  void listsTableTypes() throws Exception {
    ResultSet resultSet = metaData.getTableTypes();

    int numEntries = 0;
    while (resultSet.next()) {
      assertNotNull(resultSet.getString("TABLE_TYPE"));
      assertFalse(resultSet.getString("TABLE_TYPE").isEmpty());
      numEntries++;
    }
    assertTrue(numEntries > 0);
  }

  @Test
  void loadsColumns() throws Exception {
    ResultSet resultSet = metaData.getColumns(null, null, null, null);

    int numEntries = 0;
    while (resultSet.next()) {
      assertNotNull(resultSet.getString("TABLE_CAT"));
      assertNotNull(resultSet.getString("COLUMN_NAME"));
      numEntries++;
    }
    assertTrue(numEntries > 0);
  }

  @Test
  void loadsPrimaryKeys() throws Exception {
    ResultSet resultSet = metaData.getPrimaryKeys(null, null, null);

    int numEntries = 0;
    while (resultSet.next()) {
      assertNotNull(resultSet.getString("TABLE_CAT"));
      assertNotNull(resultSet.getString("TABLE_NAME"));
      numEntries++;
    }
    assertTrue(numEntries > 0);
  }

  @Test
  void listsTypeInfo() throws Exception {
    ResultSet resultSet = metaData.getTypeInfo();

    int numEntries = 0;
    while (resultSet.next()) {
      assertNotNull(resultSet.getString("TYPE_NAME"));
      numEntries++;
    }
    assertTrue(numEntries > 0);
  }

}
