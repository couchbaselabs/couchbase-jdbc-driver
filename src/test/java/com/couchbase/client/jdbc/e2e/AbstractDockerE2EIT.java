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

import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end checks of the packaged driver against a real cluster in Docker. Each flavor supplies the
 * cluster ({@code *DockerE2EIT} in its own test source root); the checks here are common to both.
 * <p>
 * These run under the {@code docker-e2e} failsafe execution (parent pom), which puts the <b>shaded jar</b>
 * on the classpath in place of {@code target/classes} and drops the driver's compile/runtime dependencies,
 * so the driver code under test is exactly the artifact users download. The tests therefore talk to the
 * driver only through {@code java.sql}: no driver, SDK or core-io class is referenced here.
 * <p>
 * The cluster is a sibling container when the build itself runs in a container (CI agents mount the
 * host's docker socket). Every address therefore comes from Testcontainers' {@code getHost()} and
 * {@code getMappedPort()}, never from a hard-coded loopback address.
 */
public abstract class AbstractDockerE2EIT {

  protected static final String USER = "Administrator";
  protected static final String PASSWORD = "password";

  /** The driver class registered through META-INF/services/java.sql.Driver. */
  private static final String DRIVER_CLASS = "com.couchbase.client.jdbc.CouchbaseDriver";

  /**
   * Skips (rather than fails) when there is no usable Docker environment, so that a build on a machine
   * without Docker still passes - unless Docker is required, in which case the reason Testcontainers could
   * not reach it fails the test. Docker is required when {@code -De2e.requireDocker=true}, and by default on
   * Jenkins ({@code JENKINS_URL} set), where a silently skipped end-to-end test would look like a pass.
   */
  protected static void assumeDocker() {
    boolean required = Boolean.parseBoolean(System.getProperty("e2e.requireDocker",
        String.valueOf(System.getenv("JENKINS_URL") != null)));
    if (required) {
      // Throws with Testcontainers' account of every strategy it tried.
      DockerClientFactory.instance().client();
      return;
    }
    if (!DockerClientFactory.instance().isDockerAvailable()) {
      String reason = "Docker is not available; skipping the Docker end-to-end test"
          + " (set -De2e.requireDocker=true to fail instead and see why)";
      System.err.println(reason);
      assumeTrue(false, reason);
    }
  }

  /**
   * Waits until the analytics service in {@code container} answers a query. The probe runs inside the
   * container against its own loopback, so it says nothing about the network path between the test and
   * the cluster: that path is left for the tests themselves to exercise.
   */
  protected static void awaitAnalyticsQueries(GenericContainer<?> container, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Container.ExecResult last;
    do {
      last = container.execInContainer("curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
          "-u", USER + ":" + PASSWORD, "--data-urlencode", "statement=SELECT 1",
          "http://localhost:8095/analytics/service");
      if ("200".equals(last.getStdout().trim())) {
        return;
      }
      Thread.sleep(2000);
    } while (System.nanoTime() < deadline);
    throw new IllegalStateException("analytics did not become ready within " + timeout + "; last probe: HTTP "
        + last.getStdout().trim() + " " + last.getStderr().trim());
  }

  /** The JDBC URL of the running cluster, built from the container's host and mapped port. */
  protected abstract String jdbcUrl();

  @Test
  void driverComesFromTheShadedJar() throws Exception {
    Driver driver = DriverManager.getDriver(jdbcUrl());
    assertEquals(DRIVER_CLASS, driver.getClass().getName());

    // DriverManager found the driver through the service file in the jar; nothing loaded it by name.
    boolean listed = false;
    for (Driver d : ServiceLoader.load(Driver.class)) {
      listed |= DRIVER_CLASS.equals(d.getClass().getName());
    }
    assertTrue(listed, "META-INF/services/java.sql.Driver does not list " + DRIVER_CLASS);

    String expectedJar = System.getProperty("e2e.driver.jar");
    if (expectedJar != null && !expectedJar.isEmpty()) {
      File expected = new File(expectedJar).getCanonicalFile();
      assertEquals(expected, codeSource(driver.getClass()), "driver was not loaded from the packaged jar");
      // The relocated Jackson the driver uses must be inside the same jar.
      Class<?> relocated = Class.forName(
          "org.apache.asterix.jdbc.core.deps.com.fasterxml.jackson.databind.ObjectMapper");
      assertEquals(expected, codeSource(relocated), "relocated Jackson is not in the packaged jar");
    }
  }

  @Test
  void connectQueryAndClose() throws Exception {
    Connection connection = DriverManager.getConnection(jdbcUrl(), USER, PASSWORD);
    try {
      assertFalse(connection.isClosed());
      assertTrue(connection.isValid(30), "connection is not valid");

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT 1 AS one")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("one"));
        assertFalse(rs.next());
      }

      // A small dataset, so the result has several rows and typed columns to check metadata against.
      String sql = "SELECT t.id AS id, t.name AS name, t.score AS score "
          + "FROM [{\"id\": 1, \"name\": \"alpha\", \"score\": 1.5}, "
          + "{\"id\": 2, \"name\": \"beta\", \"score\": 2.5}, "
          + "{\"id\": 3, \"name\": \"gamma\", \"score\": 3.5}] AS t "
          + "ORDER BY t.id";
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount());
        assertEquals("id", md.getColumnLabel(1));
        assertEquals("name", md.getColumnLabel(2));
        assertEquals("score", md.getColumnLabel(3));
        assertEquals(Types.BIGINT, md.getColumnType(1));
        assertEquals(Types.VARCHAR, md.getColumnType(2));
        assertEquals(Types.DOUBLE, md.getColumnType(3));

        List<String> names = new ArrayList<>();
        long idSum = 0;
        double scoreSum = 0;
        while (rs.next()) {
          idSum += rs.getLong(1);
          names.add(rs.getString("name"));
          scoreSum += rs.getDouble("score");
        }
        assertEquals(List.of("alpha", "beta", "gamma"), names);
        assertEquals(6, idSum);
        assertEquals(7.5, scoreSum, 0.0);
      }

      try (PreparedStatement ps = connection.prepareStatement("SELECT ? AS greeting, ? AS answer")) {
        ps.setString(1, "hello");
        ps.setInt(2, 42);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("hello", rs.getString("greeting"));
          assertEquals(42, rs.getInt("answer"));
          assertFalse(rs.next());
        }
      }

      try (Statement stmt = connection.createStatement()) {
        assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT WHERE FROM"));
      }
    } finally {
      connection.close();
    }
    assertTrue(connection.isClosed());
    assertFalse(connection.isValid(5), "a closed connection must not be valid");
  }

  @Test
  void databaseMetaData() throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl(), USER, PASSWORD)) {
      DatabaseMetaData md = connection.getMetaData();
      // getURL() comes from asterix-jdbc-core, which reports its own scheme (jdbc:asterixdb://host:port/...)
      // rather than the URL the caller used; check only that it names the same endpoint.
      String endpoint = jdbcUrl().substring(jdbcUrl().indexOf("//") + 2);
      assertTrue(md.getURL().contains(endpoint), "DatabaseMetaData.getURL() " + md.getURL() + " does not name " + endpoint);
      assertEquals(USER, md.getUserName());
      assertNotNull(md.getDatabaseProductName());
      assertEquals("Couchbase JDBC Driver", md.getDriverName());

      // The shaded jar's manifest carries the project version; the driver reports it from there.
      String expectedVersion = System.getProperty("e2e.driver.version");
      if (expectedVersion != null && !expectedVersion.isEmpty()) {
        assertEquals(expectedVersion, md.getDriverVersion());
        String[] parts = expectedVersion.split("[.-]");
        assertEquals(Integer.parseInt(parts[0]), md.getDriverMajorVersion());
        assertEquals(Integer.parseInt(parts[1]), md.getDriverMinorVersion());
      }

      try (ResultSet rs = md.getCatalogs()) {
        assertTrue(rs.next(), "no catalogs reported");
      }
    }
  }

  private static File codeSource(Class<?> c) throws URISyntaxException, java.io.IOException {
    return new File(c.getProtectionDomain().getCodeSource().getLocation().toURI()).getCanonicalFile();
  }
}
