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

package com.couchbase.client.jdbc;

import com.couchbase.client.jdbc.analytics.AnalyticsDataSource;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Logger;

public final class CouchbaseDriver implements Driver {

  /**
   * The default hostname used if not present.
   */
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";

  /**
   * The name of this driver.
   */
  public static final String DRIVER_NAME = "Couchbase JDBC Driver";

  /**
   * The product name used for the database/driver.
   */
  public static final String PRODUCT_NAME = "Couchbase Server";

  /**
   * The supported JDBC major version.
   */
  public static final int JDBC_MAJOR_VERSION = 4;

  /**
   * The supported JDBC minor version.
   */
  public static final int JDBC_MINOR_VERSION = 2;

  private static final Logger PARENT_LOGGER = Logger.getLogger("com.couchbase.client.jdbc");
  private static final Logger LOGGER = Logger.getLogger(CouchbaseDriver.class.getCanonicalName());

  public static final String QUERY_URL_PREFIX = "jdbc:couchbase:query";
  public static final String ANALYTICS_URL_PREFIX = "jdbc:couchbase:analytics";

  public static final AtomicReference<String> DRIVER_VERSION = new AtomicReference<>("");
  public static final AtomicInteger DRIVER_MAJOR_VERSION = new AtomicInteger(0);
  public static final AtomicInteger DRIVER_MINOR_VERSION = new AtomicInteger(0);

  static {
    try {
      register();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    loadManifestVersions();
  }

  private static void loadManifestVersions() {
    try {
      Enumeration<URL> resources = CouchbaseDriver.class.getClassLoader().getResources(JarFile.MANIFEST_NAME);
      while (resources.hasMoreElements()) {
        URL manifestUrl = resources.nextElement();
        if (manifestUrl == null) {
          continue;
        }
        Manifest manifest = new Manifest(manifestUrl.openStream());
        if (manifest.getEntries() == null) {
          continue;
        }

        for (Map.Entry<String, Attributes> entry : manifest.getEntries().entrySet()) {
          if (entry.getKey().startsWith("couchbase-jdbc-driver")) {
            String version = entry.getValue().getValue("Impl-Version");
            if (version != null && !version.isEmpty()) {
              DRIVER_VERSION.set(version);
              String[] parts = version.split("\\.");
              if (parts[0] != null && parts[1] != null) {
                DRIVER_MAJOR_VERSION.set(Integer.parseInt(parts[0]));
                DRIVER_MINOR_VERSION.set(Integer.parseInt(parts[1]));
              }
            }
          }
        }
      }
    } catch (Exception e) {
      // Ignored on purpose.
    }
  }

  private static void register() throws SQLException {
    DriverManager.registerDriver(new CouchbaseDriver());
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (url == null) {
      throw new SQLException("url is null");
    }

    if (!acceptsURL(url)) {
      return null;
    }

    HostComponents components = extractHostComponents(url);

    String hostname = components.hostname();
    Properties merged = mergeUrlArgs(url, info);

    // if (url.startsWith(QUERY_URL_PREFIX)) {
    //   return new QueryDataSource().getConnection();
    // } else
    if (url.startsWith(ANALYTICS_URL_PREFIX)) {
      return AnalyticsDataSource
        .builder()
        .hostname(hostname)
        .properties(merged)
        .catalog(components.catalog())
        .schema(components.schema())
        .url(url)
        .build()
        .getConnection();
    } else {
      return null;
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(ANALYTICS_URL_PREFIX);
    // return url.startsWith(QUERY_URL_PREFIX) || url.startsWith(ANALYTICS_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    Properties copy = new Properties(info);

    CouchbaseDriverProperty[] knownProperties = CouchbaseDriverProperty.values();
    DriverPropertyInfo[] props = new DriverPropertyInfo[knownProperties.length];
    for (int i = 0; i < props.length; ++i) {
      props[i] = knownProperties[i].toDriverPropertyInfo(copy);
    }
    return props;
  }

  @Override
  public int getMajorVersion() {
    return DRIVER_MAJOR_VERSION.get();
  }

  @Override
  public int getMinorVersion() {
    return DRIVER_MINOR_VERSION.get();
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() {
    return PARENT_LOGGER;
  }

  private static HostComponents extractHostComponents(final String url) {
    String urlServer = url;

    int qPos = url.indexOf('?');
    if (qPos != -1) {
      urlServer = url.substring(0, qPos);
    }

    if (urlServer.startsWith(ANALYTICS_URL_PREFIX)) {
      urlServer = urlServer.substring(ANALYTICS_URL_PREFIX.length());
    } else {
      urlServer = urlServer.substring(QUERY_URL_PREFIX.length());
    }

    if (urlServer.startsWith("://")) {
      urlServer = urlServer.substring("://".length());
    }

    String hostname = DEFAULT_HOSTNAME;
    String catalog = null;
    String schema = null;

    String[] urlComponents = urlServer.split("/");
    if (urlComponents.length >= 1) {
      hostname = urlComponents[0];
    }
    if(urlComponents.length >= 2) {
      catalog = urlComponents[1];
    }
    if (urlComponents.length >= 3) {
      schema = urlComponents[2];
    }
    return new HostComponents(hostname, catalog, schema);
  }

  private static Properties mergeUrlArgs(final String url, final Properties defaults) {
    Properties urlProps = new Properties(defaults);

    String urlArgs = "";

    int qPos = url.indexOf('?');
    if (qPos != -1) {
      urlArgs = url.substring(qPos + 1);
    }

    // parse the args part of the url
    String[] args = urlArgs.split("&");
    for (String token : args) {
      if (token.isEmpty()) {
        continue;
      }
      int pos = token.indexOf('=');
      if (pos == -1) {
        urlProps.setProperty(token, "");
      } else {
        try {
          urlProps.setProperty(
            token.substring(0, pos),
            URLDecoder.decode(token.substring(pos + 1), StandardCharsets.UTF_8.name())
          );
        } catch (UnsupportedEncodingException e) {
          throw new IllegalStateException("Unsupported Encoding", e);
        }
      }
    }

    return urlProps;
  }

  static class HostComponents {
    private final String hostname;
    private final String catalog;
    private final String schema;

    public HostComponents(String hostname, String catalog, String schema) {
      this.hostname = hostname;
      this.catalog = catalog;
      this.schema = schema;
    }

    public String hostname() {
      return hostname;
    }

    public String catalog() {
      return catalog;
    }

    public String schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "HostComponents{" +
        "hostname='" + hostname + '\'' +
        ", catalog='" + catalog + '\'' +
        ", schema='" + schema + '\'' +
        '}';
    }
  }

}
