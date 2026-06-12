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

import java.io.PrintStream;
import java.util.Objects;

/**
 * Runnable example used by unit tests to verify expected console output
 * and that both "direct SDK" and "JDBC driver" paths attempt a connect+query.
 */
public class AnalyticsSdkAndJdbcDriverExample {

  static final String TEST_QUERY = "SELECT 1";

  @FunctionalInterface
  public interface DirectSdkTest {
    void run(PrintStream out, String query) throws Exception;
  }

  @FunctionalInterface
  public interface JdbcDriverTest {
    void run(PrintStream out, String query) throws Exception;
  }

  public static void run(final PrintStream out, final DirectSdkTest directSdk, final JdbcDriverTest jdbcDriver)
    throws Exception {
    Objects.requireNonNull(out, "out");
    Objects.requireNonNull(directSdk, "directSdk");
    Objects.requireNonNull(jdbcDriver, "jdbcDriver");

    out.println("=== Couchbase Analytics SDK & JDBC Driver Test ===\n");

    // Test 1: Direct SDK Usage
    out.println("--- Test 1: Direct Analytics SDK Usage ---");
    directSdk.run(out, TEST_QUERY);

    // Test 2: JDBC Driver Usage
    out.println("\n--- Test 2: JDBC Driver Usage ---");
    jdbcDriver.run(out, TEST_QUERY);

    out.println("\n=== All Tests Complete ===");
  }

  public static void main(final String[] args) throws Exception {
    run(System.out, (out, query) -> out.println("(direct SDK) would connect and run: " + query),
      (out, query) -> out.println("(JDBC) would connect and run: " + query));
  }
}
