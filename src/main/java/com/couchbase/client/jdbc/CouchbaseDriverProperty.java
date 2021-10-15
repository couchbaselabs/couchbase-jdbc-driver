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

public enum CouchbaseDriverProperty {
  PASSWORD(
    "password",
    null,
    true,
    "Password to use when authenticating."
  ),
  USER(
    "user",
    null,
    true,
    "Username to connect to the database as"
  ),
  CATALOG_DATAVERSE_MODE(
    "catalogDataverseMode",
    "catalog",
    false,
    "Defines how the catalog should be interpreted. With \"catalog\" both bucket and scope are used " +
      "for the catalog, while with \"catalogSchema\" the bucket is used as a catalog and the scope as the schema.",
    new String[] { "catalog", "catalogSchema" }
  ),
  CATALOG_INCLUDE_SCHEMALESS(
    "catalogIncludesSchemaless",
    "false",
    false,
    "If the Catalog API should also include schemaless catalogs."
  ),
  MAX_WARNINGS(
    "maxWarnings",
    "10",
    false,
    "The maximum number of warnings that should be emitted."
  ),
  SQL_COMPAT_MODE(
    "sqlCompatMode",
    "true",
    false,
    "If the analytics SQL compatibility mode should be used."
  ),
  SSL(
    "ssl",
    "false",
    false,
    "Set to true if transport encryption (TLS) should be enabled."
  ),
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
  SSL_CERT_PATH(
    "sslCertPath",
    null,
    false,
    "The absolute path to the TLS certificate."
  ),
  SSL_KEYSTORE_PATH(
    "sslKeystorePath",
    null,
    false,
    "The absolute path to the java keystore."
  ),
  SSL_KEYSTORE_PASSWORD(
    "sslKeystorePassword",
    null,
    false,
    "The password for the keystore."
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

  public String get(Properties properties) {
    return properties.getProperty(name, defaultValue);
  }

  public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
    DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, get(properties));
    propertyInfo.required = required;
    propertyInfo.description = description;
    propertyInfo.choices = choices;
    return propertyInfo;
  }

  public String getName() {
    return name;
  }
}
