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
  )
  ;

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
