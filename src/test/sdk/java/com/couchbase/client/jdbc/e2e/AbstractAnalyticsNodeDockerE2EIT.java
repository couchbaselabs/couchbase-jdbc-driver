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

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Runs the couchbase-operational-insights-jdbc-driver flavor against a single-node analytics product (Operational
 * Insights or Enterprise Analytics), both of which keep their data in blob storage.
 * <p>
 * The node gets an S3 mock on a private Docker network, reached by its network alias. Node and mock talk to each
 * other over that network, so nothing depends on where the test JVM runs. The test reaches the node only
 * through {@code getHost()} and the mapped analytics port: the Analytics SDK is a plain HTTP client of that one
 * endpoint and reads no node addresses from the cluster, so it needs no alternate-address setup.
 * <p>
 * Nothing is bind-mounted. Setup runs {@code couchbase-cli} inside the node, so it needs no host path and no
 * network path from the test.
 */
abstract class AbstractAnalyticsNodeDockerE2EIT extends AbstractDockerE2EIT {

  private static final String S3MOCK_IMAGE = System.getProperty("e2e.s3mock.image", "adobe/s3mock:4.12.4");
  private static final String S3MOCK_ALIAS = "s3mock";
  private static final String BUCKET = "jdbc-e2e";
  private static final int ANALYTICS_PORT = 8095;
  private static final int MGMT_PORT = 8091;
  private static final int CBAS_QUOTA_MB = Integer.getInteger("cbas.quota", 1200);

  /** The running node and its supporting containers; one per concrete test class. */
  static final class Cluster implements AutoCloseable {
    private final Network network;
    private final GenericContainer<?> s3;
    final GenericContainer<?> node;

    private Cluster(Network network, GenericContainer<?> s3, GenericContainer<?> node) {
      this.network = network;
      this.s3 = s3;
      this.node = node;
    }

    String jdbcUrl() {
      return "jdbc:couchbase:analytics://" + node.getHost() + ":" + node.getMappedPort(ANALYTICS_PORT);
    }

    @Override
    public void close() {
      node.stop();
      s3.stop();
      network.close();
    }
  }

  static Cluster startCluster(String image) throws Exception {
    assumeDocker();
    Network network = Network.newNetwork();
    GenericContainer<?> s3 = new GenericContainer<>(DockerImageName.parse(S3MOCK_IMAGE))
        .withNetwork(network)
        .withNetworkAliases(S3MOCK_ALIAS)
        .withEnv("initialBuckets", BUCKET)
        .withExposedPorts(9090)
        .waitingFor(Wait.forHttp("/").forPort(9090).forStatusCode(200));
    GenericContainer<?> node = new GenericContainer<>(DockerImageName.parse(image))
        .withNetwork(network)
        .withExposedPorts(MGMT_PORT, ANALYTICS_PORT)
        .waitingFor(Wait.forHttp("/pools").forPort(MGMT_PORT).forStatusCode(200))
        .withStartupTimeout(Duration.ofMinutes(5));
    Cluster cluster = new Cluster(network, s3, node);
    try {
      s3.start();
      node.start();
      // Blob storage has to be configured before the cluster is initialised. Operational Insights deprecates
      // this command in favour of setting-operational-insights but still accepts it, so one command covers both.
      cli(node, "setting-enterprise-analytics", "-c", "127.0.0.1:" + MGMT_PORT, "-u", USER, "-p", PASSWORD,
          "--set", "--scheme", "s3", "--bucket", BUCKET, "--region", "us-east-1",
          "--endpoint", "http://" + S3MOCK_ALIAS + ":9090", "--anonymous-auth", "1",
          "--path-style-addressing", "1", "--partitions", "8");
      cli(node, "cluster-init", "-c", "127.0.0.1:" + MGMT_PORT, "--cluster-username", USER,
          "--cluster-password", PASSWORD, "--cluster-analytics-ramsize", String.valueOf(CBAS_QUOTA_MB));
      awaitAnalyticsQueries(node, Duration.ofMinutes(3));
      return cluster;
    } catch (Exception | Error e) {
      String logs = node.getContainerId() != null ? tail(node.getLogs(), 4000) : "(node not created)";
      cluster.close();
      throw new IllegalStateException("could not start " + image + "; node log tail:\n" + logs, e);
    }
  }

  private static void cli(GenericContainer<?> node, String... args) throws Exception {
    String[] command = new String[args.length + 1];
    command[0] = "couchbase-cli";
    System.arraycopy(args, 0, command, 1, args.length);
    Container.ExecResult result = node.execInContainer(command);
    if (result.getExitCode() != 0 || !result.getStdout().contains("SUCCESS")) {
      throw new IllegalStateException("couchbase-cli " + args[0] + " failed (exit " + result.getExitCode() + "): "
          + result.getStdout() + result.getStderr());
    }
  }

  private static String tail(String s, int n) {
    return s.length() <= n ? s : s.substring(s.length() - n);
  }
}
