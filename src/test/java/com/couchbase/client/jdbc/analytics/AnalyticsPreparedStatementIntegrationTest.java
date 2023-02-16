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
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnalyticsPreparedStatementIntegrationTest extends BaseAnalyticsIntegrationTest {

  private static Connection connection;

  @BeforeAll
  static void setup() throws Exception {
    startContainer(AnalyticsPreparedStatementIntegrationTest.class);
    connection = DriverManager.getConnection(url(), username(), password());
  }

  @AfterAll
  static void teardown() throws Exception {
    connection.close();
    stopContainer();
  }

  @Test
  void executeQuery() throws Exception {
    String statement = "select `Dataverse`.* from Metadata.`Dataverse` where DataverseName = ?";
    PreparedStatement preparedStatement = connection.prepareStatement(statement);

    assertEquals(1, preparedStatement.getParameterMetaData().getParameterCount());
    assertTrue(preparedStatement.getMetaData().getColumnCount() > 0);

    preparedStatement.setString(1, "Default");
    ResultSet resultSet = preparedStatement.executeQuery();

    assertTrue(resultSet.next());
    assertEquals("Default", resultSet.getString("DataverseName"));
    assertNotNull(resultSet.getString("Timestamp"));
    assertTrue(resultSet.getMetaData().getColumnCount() > 0);

    assertFalse(resultSet.next());
  }

}
