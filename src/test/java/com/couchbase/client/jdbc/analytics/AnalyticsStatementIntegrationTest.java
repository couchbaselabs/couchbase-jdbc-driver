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
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnalyticsStatementIntegrationTest extends BaseAnalyticsIntegrationTest {

  private static Connection connection;

  @BeforeAll
  static void setup() throws Exception {
    connection = DriverManager.getConnection(url(), username(), password());
  }

  @AfterAll
  static void teardown() throws Exception {
    connection.close();
  }

 @Test
  void performsSimpleExecuteQuery() throws Exception {
   ResultSet resultSet = connection.createStatement().executeQuery("select \"Michael\" as firstname");
   resultSet.next();
   assertEquals("Michael", resultSet.getString("firstname"));
   assertEquals("Michael", resultSet.getString(1));
 }

  @Test
  void performsSimpleExecute() throws Exception {
    Statement statement = connection.createStatement();
    assertNotNull(statement);

    boolean result = statement.execute("select \"Michael\" as firstname");
    assertTrue(result);

    ResultSet resultSet = statement.getResultSet();
    resultSet.next();
    assertEquals("Michael", resultSet.getString("firstname"));
    assertEquals("Michael", resultSet.getString(1));
  }

  @Test
  void raisesStatementErrors() throws Exception {
    Statement statement = connection.createStatement();
    assertNotNull(statement);

    boolean result = statement.execute("select 1=");
    assertTrue(result);

    ResultSet resultSet = statement.getResultSet();
  }

}
