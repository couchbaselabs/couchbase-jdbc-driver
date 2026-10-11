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

package com.couchbase.client.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests the shaded driver jar, after relocation: the SDK's slf4j logging must reach
 * java.util.logging whatever slf4j the application has, and must not disturb it.
 * Run by failsafe, which supplies the jar and the application slf4j jars.
 */
class ShadedJarLoggingIT {

  private static final String DEPS = "com.couchbase.client.jdbc.deps.";
  private static final Path SHADED_JAR = Paths.get(System.getProperty("shaded.jar"));
  private static final Path APP_SLF4J_DIR = Paths.get(System.getProperty("app.slf4j.dir"));

  @Test
  void jarHasNoUnrelocatedSlf4j() throws Exception {
    List<String> leaked;
    try (ZipFile jar = new ZipFile(SHADED_JAR.toFile())) {
      leaked = jar.stream()
          .map(ZipEntry::getName)
          .filter(name -> name.startsWith("org/slf4j/") || name.startsWith("META-INF/services/org.slf4j."))
          .collect(Collectors.toList());
    }
    assertEquals(Collections.emptyList(), leaked);
  }

  @ParameterizedTest(name = "application slf4j: {0}")
  @ValueSource(strings = {"none", "1.7.36", "2.0.18"})
  void sdkLogsReachJavaUtilLogging(String appSlf4j) throws Exception {
    List<URL> urls = new ArrayList<>();
    if (!appSlf4j.equals("none")) {
      // Ahead of the driver, so the application's slf4j wins any contest for org.slf4j.
      urls.add(appJar("slf4j-api", appSlf4j));
      urls.add(appJar("slf4j-simple", appSlf4j));
    }
    urls.add(SHADED_JAR.toUri().toURL());

    // The platform loader as parent hides the test classpath, which has the unshaded deps.
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);

      Object factory = loader.loadClass(DEPS + "org.slf4j.LoggerFactory").getMethod("getILoggerFactory").invoke(null);
      assertTrue(factory.getClass().getName().startsWith(DEPS), factory.getClass().getName());
      assertEquals("JDK14LoggerFactory", factory.getClass().getSimpleName());

      if (!appSlf4j.equals("none")) {
        Object appFactory = loader.loadClass("org.slf4j.LoggerFactory").getMethod("getILoggerFactory").invoke(null);
        assertEquals("SimpleLoggerFactory", appFactory.getClass().getSimpleName());
      }

      List<LogRecord> records = new CopyOnWriteArrayList<>();
      Handler handler = new Handler() {
        @Override
        public void publish(LogRecord record) {
          records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
      };
      Logger root = Logger.getLogger("");
      root.addHandler(handler);
      String[] expected;
      try {
        expected = warnFromSdk(loader);
      } finally {
        root.removeHandler(handler);
      }
      assertTrue(records.stream().anyMatch(r -> r.getLevel() == Level.WARNING
              && r.getLoggerName().equals(expected[0])
              && r.getMessage().contains(expected[1])),
          "no WARNING from " + expected[0] + " containing '" + expected[1] + "' in "
              + records.stream().map(r -> r.getLoggerName() + ": " + r.getMessage()).collect(Collectors.toList()));
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static URL appJar(String artifactId, String version) throws Exception {
    Path jar = APP_SLF4J_DIR.resolve(artifactId + "-" + version + ".jar");
    assertTrue(Files.isRegularFile(jar), jar.toString());
    return jar.toUri().toURL();
  }

  /**
   * Has the flavor's SDK log a warning through its own code path.
   *
   * @return the expected java.util.logging logger name and a fragment of the message
   */
  private static String[] warnFromSdk(ClassLoader loader) throws Exception {
    if (hasClass(loader, "com.couchbase.client.core.cnc.LoggingEventConsumer")) {
      // Analytics flavor: core-io turns each event into an slf4j call on a logger named by its category.
      String category = "com.couchbase.jdbc.it";
      Class<?> eventType = loader.loadClass("com.couchbase.client.core.cnc.Event");
      Object warn = loader.loadClass("com.couchbase.client.core.cnc.Event$Severity").getField("WARN").get(null);
      Object event = Proxy.newProxyInstance(loader, new Class<?>[] {eventType}, (proxy, method, args) -> {
        switch (method.getName()) {
          case "severity":
            return warn;
          case "category":
            return category;
          case "description":
            return "from core-io";
          case "duration":
            return Duration.ZERO;
          case "created":
            return Instant.now();
          case "createdAt":
            return System.nanoTime();
          case "hashCode":
            return System.identityHashCode(proxy);
          case "equals":
            return proxy == args[0];
          default:
            return null;
        }
      });
      Object consumer = loader.loadClass("com.couchbase.client.core.cnc.LoggingEventConsumer")
          .getMethod("create").invoke(null);
      consumer.getClass().getMethod("accept", Object.class).invoke(consumer, event);
      return new String[] {category, "from core-io"};
    }
    String versionAndGitHash = "com.couchbase.analytics.client.java.internal.utils.VersionAndGitHash";
    if (hasClass(loader, versionAndGitHash)) {
      // Operational Insights flavor: the Analytics SDK warns when a package has no version.properties.
      loader.loadClass(versionAndGitHash).getMethod("from", Class.class).invoke(null, Object.class);
      return new String[] {versionAndGitHash, "version.properties"};
    }
    return fail("neither core-io nor the Analytics SDK is in " + SHADED_JAR);
  }

  private static boolean hasClass(ClassLoader loader, String name) {
    try {
      loader.loadClass(name);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
