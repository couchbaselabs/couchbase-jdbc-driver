package com.couchbase.client.jdbc.analytics;

import com.couchbase.analytics.client.java.Cluster;
import com.couchbase.analytics.client.java.Credential;
import com.couchbase.analytics.client.java.InternalUnsupportedHttpClient;
import com.couchbase.analytics.client.java.Row;
import com.couchbase.analytics.client.java.internal.RawQueryMetadata;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Test to understand how lossless-adm format works with executeStreaming.
 * Run manually to see the output.
 */
public class LosslessAdmTest {

    public static void main(String[] args) {
        String connectionString = "http://localhost:9600";
        String username = "couchbase";
        String password = "couchbase";

        try (Cluster cluster = Cluster.newInstance(
            connectionString,
            Credential.of(username, password)
        )) {
            Duration timeout = Duration.ofSeconds(30);

            System.out.println("=".repeat(60));
            System.out.println("Testing executeStreaming with lossless-adm=true");
            System.out.println("=".repeat(60));

            testWithLosslessAdm(cluster, timeout);

            System.out.println();
            System.out.println("=".repeat(60));
            System.out.println("Testing executeStreaming WITHOUT lossless-adm");
            System.out.println("=".repeat(60));

            testWithoutLosslessAdm(cluster, timeout);

            System.out.println();
            System.out.println("=".repeat(60));
            System.out.println("Testing execute (full response) with lossless-adm=true");
            System.out.println("=".repeat(60));

            testExecuteWithLosslessAdm(cluster, timeout);

            System.out.println();
            System.out.println("=".repeat(60));
            System.out.println("Testing execute (full response) WITHOUT lossless-adm");
            System.out.println("=".repeat(60));

            testExecuteWithoutLosslessAdm(cluster, timeout);
        }
    }

    static void testWithLosslessAdm(Cluster cluster, Duration timeout) {
        InternalUnsupportedHttpClient httpClient = InternalUnsupportedHttpClient.from(cluster);

        List<Row> resultRows = new ArrayList<>();
        Consumer<Row> rowCallback = resultRows::add;

        String requestJson = "{\"statement\":\"SELECT 'hello' as str, 123 as num, 3.14 as dec\",\"sql-compat\":true}";

        RawQueryMetadata metadata = httpClient.executeStreaming(
            req -> req
                .path("/api/v1/request")
                .header("Accept", "application/json; charset=UTF-8; lossless-adm=true")
                .postJson(requestJson.getBytes(StandardCharsets.UTF_8)),
            timeout,
            rowCallback,
            null
        );

        System.out.println("Request: " + requestJson);
        System.out.println("Metadata status: " + metadata.status);
        System.out.println("Signature bytes: " + (metadata.signature != null ? new String(metadata.signature, StandardCharsets.UTF_8) : "null"));
        System.out.println("Row count: " + resultRows.size());
        for (int i = 0; i < resultRows.size(); i++) {
            Row row = resultRows.get(i);
            String rowJson = new String(row.bytes(), StandardCharsets.UTF_8);
            System.out.println("Row " + i + " bytes: " + rowJson);
        }
    }

    static void testWithoutLosslessAdm(Cluster cluster, Duration timeout) {
        InternalUnsupportedHttpClient httpClient = InternalUnsupportedHttpClient.from(cluster);

        List<Row> resultRows = new ArrayList<>();
        Consumer<Row> rowCallback = resultRows::add;

        String requestJson = "{\"statement\":\"SELECT 'hello' as str, 123 as num, 3.14 as dec\",\"sql-compat\":true}";

        RawQueryMetadata metadata = httpClient.executeStreaming(
            req -> req
                .path("/api/v1/request")
                .header("Accept", "application/json; charset=UTF-8")
                .postJson(requestJson.getBytes(StandardCharsets.UTF_8)),
            timeout,
            rowCallback,
            null
        );

        System.out.println("Request: " + requestJson);
        System.out.println("Metadata status: " + metadata.status);
        System.out.println("Signature bytes: " + (metadata.signature != null ? new String(metadata.signature, StandardCharsets.UTF_8) : "null"));
        System.out.println("Row count: " + resultRows.size());
        for (int i = 0; i < resultRows.size(); i++) {
            Row row = resultRows.get(i);
            String rowJson = new String(row.bytes(), StandardCharsets.UTF_8);
            System.out.println("Row " + i + " bytes: " + rowJson);
        }
    }

    static void testExecuteWithLosslessAdm(Cluster cluster, Duration timeout) {
        InternalUnsupportedHttpClient httpClient = InternalUnsupportedHttpClient.from(cluster);

        String requestJson = "{\"statement\":\"SELECT 'hello' as str, 123 as num, 3.14 as dec\",\"sql-compat\":true}";

        try (InternalUnsupportedHttpClient.Response response = httpClient.execute(
            req -> req
                .path("/api/v1/request")
                .header("Accept", "application/json; charset=UTF-8; lossless-adm=true")
                .postJson(requestJson.getBytes(StandardCharsets.UTF_8)),
            timeout
        )) {
            System.out.println("Request: " + requestJson);
            System.out.println("HTTP Status: " + response.httpStatusCode());
            String body = response.bodyAsString();
            System.out.println("Full response body:");
            System.out.println(body);
        }
    }

    static void testExecuteWithoutLosslessAdm(Cluster cluster, Duration timeout) {
        InternalUnsupportedHttpClient httpClient = InternalUnsupportedHttpClient.from(cluster);

        String requestJson = "{\"statement\":\"SELECT 'hello' as str, 123 as num, 3.14 as dec\",\"sql-compat\":true}";

        try (InternalUnsupportedHttpClient.Response response = httpClient.execute(
            req -> req
                .path("/api/v1/request")
                .header("Accept", "application/json; charset=UTF-8")
                .postJson(requestJson.getBytes(StandardCharsets.UTF_8)),
            timeout
        )) {
            System.out.println("Request: " + requestJson);
            System.out.println("HTTP Status: " + response.httpStatusCode());
            String body = response.bodyAsString();
            System.out.println("Full response body:");
            System.out.println(body);
        }
    }
}
