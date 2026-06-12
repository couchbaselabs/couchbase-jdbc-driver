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

package com.couchbase.client.jdbc.examples;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AnalyticsSdkAndJdbcDriverExampleTest {

  @Test
  void printsExpectedBannerAndRunsBothConnectivityAndQueryPaths() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos, true, StandardCharsets.UTF_8);

    class Recorder {
      boolean connected;
      boolean queried;
      String query;
    }

    Recorder sdk = new Recorder();
    Recorder jdbc = new Recorder();

    AnalyticsSdkAndJdbcDriverExample.run(out,
      (ps, query) -> {
        sdk.connected = true;
        sdk.queried = true;
        sdk.query = query;
        ps.println("(direct SDK) connected");
        ps.println("(direct SDK) query OK");
      },
      (ps, query) -> {
        jdbc.connected = true;
        jdbc.queried = true;
        jdbc.query = query;
        ps.println("(JDBC) connected");
        ps.println("(JDBC) query OK");
      }
    );

    String output = baos.toString(StandardCharsets.UTF_8);

    assertTrue(output.contains("=== Couchbase Analytics SDK & JDBC Driver Test ==="));
    assertTrue(output.contains("--- Test 1: Direct Analytics SDK Usage ---"));
    assertTrue(output.contains("--- Test 2: JDBC Driver Usage ---"));
    assertTrue(output.contains("=== All Tests Complete ==="));

    assertTrue(sdk.connected);
    assertTrue(sdk.queried);
    assertTrue(AnalyticsSdkAndJdbcDriverExample.TEST_QUERY.equals(sdk.query));

    assertTrue(jdbc.connected);
    assertTrue(jdbc.queried);
    assertTrue(AnalyticsSdkAndJdbcDriverExample.TEST_QUERY.equals(jdbc.query));

    int bannerIdx = output.indexOf("=== Couchbase Analytics SDK & JDBC Driver Test ===");
    int test1Idx = output.indexOf("--- Test 1: Direct Analytics SDK Usage ---");
    int test2Idx = output.indexOf("--- Test 2: JDBC Driver Usage ---");
    int doneIdx = output.indexOf("=== All Tests Complete ===");
    assertTrue(bannerIdx >= 0 && test1Idx > bannerIdx && test2Idx > test1Idx && doneIdx > test2Idx,
      "Expected output to be in order, but was:\n" + output);
  }
}
