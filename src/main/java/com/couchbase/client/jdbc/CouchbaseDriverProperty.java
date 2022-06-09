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

import java.sql.DriverPropertyInfo;
import java.util.Properties;

/**
 * Properties with which the JDBC driver can be configured with.
 */
public enum CouchbaseDriverProperty {
  /**
   * The password to authenticate with.
   */
  PASSWORD(
    "password",
    null,
    true,
    "Password to use when authenticating."
  ),
  /**
   * The username to authenticate with.
   */
  USER(
    "user",
    null,
    true,
    "Username to connect to the database as"
  ),
  /**
   * Connect timeout that is waited until the connection is established.
   */
  CONNECT_TIMEOUT(
    "connectTimeout",
    null,
    false,
    "If set to a value, the amount of time the driver waits during bootstrap to establish connections " +
      "properly before giving up. If not provided the lazy bootstrap continues in the background and likely the " +
      "operation will time out at the configured value (by default 75s). The format to be used is \"10s\" or similar."
  ),
  /**
   * The dataverse mode for Couchbase Analytics.
   */
  CATALOG_DATAVERSE_MODE(
    "catalogDataverseMode",
    "catalog",
    false,
    "Defines how the catalog should be interpreted. With \"catalog\" both bucket and scope are used " +
      "for the catalog, while with \"catalogSchema\" the bucket is used as a catalog and the scope as the schema.",
    new String[] { "catalog", "catalogSchema" }
  ),
  /**
   * If the Catalog API should also include schemaless catalogs.
   */
  CATALOG_INCLUDE_SCHEMALESS(
    "catalogIncludesSchemaless",
    "false",
    false,
    "If the Catalog API should also include schemaless catalogs."
  ),
  /**
   * The maximum number of warnings that should be emitted.
   */
  MAX_WARNINGS(
    "maxWarnings",
    "10",
    false,
    "The maximum number of warnings that should be emitted."
  ),
  /**
   * If Couchbase Analytics should use its SQL compatibility mode.
   */
  SQL_COMPAT_MODE(
    "sqlCompatMode",
    "true",
    false,
    "If the analytics SQL compatibility mode should be used."
  ),
  /**
   * Minimally required driver version.
   */
  MIN_DRIVER_VERSION(
    "minDriverVersion",
    null,
    false,
    "Minimally required driver version."
  ),
  /**
   * The minimally required database version.
   */
  MIN_DATABASE_VERSION(
    "minDatabaseVersion",
    null,
    false,
    "Minimally required database version."
  ),
  /**
   * If SSL/TLS should be enabled.
   */
  SSL(
    "ssl",
    "false",
    false,
    "Set to true if transport encryption (TLS) should be enabled."
  ),
  /**
   * Which TLS mode should be used (see enum for details).
   */
  SSL_MODE(
    "sslMode",
    "verify-full",
    false,
    "The mode used on how the certificate and/or the hostnames are verified.",
    new String[] {
      "verify-full", // performs certificate and hostname verification
      "verify-ca",  // performs only certificate verification
      "no-verify", // does not perform any verification (accepts all certs and hosts) - INSECURE!
    }
  ),
  /**
   * The path to the SSL certificate.
   */
  SSL_CERT_PATH(
    "sslCertPath",
    null,
    false,
    "The absolute path to the TLS certificate."
  ),
  /**
   * The path to the SSL keystore.
   */
  SSL_KEYSTORE_PATH(
    "sslKeystorePath",
    null,
    false,
    "The absolute path to the java keystore."
  ),
  /**
   * The SSL keystore password.
   */
  SSL_KEYSTORE_PASSWORD(
    "sslKeystorePassword",
    null,
    false,
    "The password for the keystore."
  ),
  /**
   * The scan consistency which should be used for queries.
   */
  SCAN_CONSISTENCY(
    "scanConsistency",
    null,
    false,
    "The scanConsistency to use for a query.",
    new String[] {
      "notBounded",
      "requestPlus",
    }
  ),
  /**
   * The scan wait which should be used for queries.
   */
  SCAN_WAIT(
    "scanWait",
    null,
    false,
    "The scanWait value to use for a query."
  );

  private final String name;
  private final String defaultValue;
  private final boolean required;
  private final String description;
  private final String[] choices;

  CouchbaseDriverProperty(String name, String defaultValue, boolean required, String description) {
    this(name, defaultValue, required, description, null);
  }

  CouchbaseDriverProperty(String name, String defaultValue, boolean required, String description, String[] choices) {
    this.name = name;
    this.defaultValue = defaultValue;
    this.required = required;
    this.description = description;
    this.choices = choices;
  }

  /**
   * Returns the property out of the given properties object.
   *
   * @param properties the properties input.
   * @return the returned property or the default value.
   */
  public String get(Properties properties) {
    return properties.getProperty(name, defaultValue);
  }

  /**
   * Turns properties into {@link DriverPropertyInfo}.
   *
   * @param properties the input properties.
   * @return the build property info.
   */
  public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
    DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, get(properties));
    propertyInfo.required = required;
    propertyInfo.description = description;
    propertyInfo.choices = choices;
    return propertyInfo;
  }

  /**
   * Returns the name of the property.
   *
   * @return the name of the property.
   */
  public String getName() {
    return name;
  }
}
