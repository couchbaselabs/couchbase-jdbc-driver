package com.couchbase.client.jdbc.analytics;

import com.couchbase.analytics.client.java.Cluster;
import com.couchbase.analytics.client.java.Credential;
import com.couchbase.analytics.client.java.QueryResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class AnalyticsSdkDirectIT {

  @Test
  public void testDirectSdkCall() {
    String connectionString = "http://localhost:9600";
    String username = "couchbase";
    String password = "couchbase";

    try (Cluster cluster = Cluster.newInstance(
        connectionString,
        Credential.of(username, password),
        clusterOptions -> clusterOptions
            .timeout(it -> it.queryTimeout(Duration.ofMinutes(2)))
    )) {
      String statement = "SELECT 'hello' as greeting";
      Map<String, String> additionalParams = new HashMap<>();
      additionalParams.put("sql-compat", "true");

      QueryResult queryResult = cluster.executeQuery(statement, options -> options.raw(additionalParams));
      queryResult.rows().forEach(row -> {
        System.out.println("executeQuery Row: " + new String(row.bytes()));
      });
    }
  }

  @Test
  public void testStreamingCall() {
    String connectionString = "http://localhost:9600";
    String username = "couchbase";
    String password = "couchbase";

    try (Cluster cluster = Cluster.newInstance(
        connectionString,
        Credential.of(username, password),
        clusterOptions -> clusterOptions
            .timeout(it -> it.queryTimeout(Duration.ofMinutes(2)))
    )) {
      String statement = "SELECT 1 as my_count";
      Map<String, String> additionalParams = new HashMap<>();
      additionalParams.put("sql-compat", "true");

      cluster.executeStreamingQuery(statement, row -> {
        System.out.println("executeStreamingQuery Row: " + new String(row.bytes()));
      }, options -> options.raw(additionalParams));
    }
  }
}
