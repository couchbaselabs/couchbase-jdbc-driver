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

package com.couchbase.client.jdbc.sdk;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;

import java.util.Properties;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A {@link ConnectionCoordinate} provides a "pointer" to a cluster managed by the SDK.
 */
public class ConnectionCoordinate {

  private final String connectionString;
  private final Authenticator authenticator;
  private final Properties properties;

  public static ConnectionCoordinate create(String connectionString, String username, String password, Properties properties) {
    return new ConnectionCoordinate(connectionString, PasswordAuthenticator.create(username, password), username, properties);
  }

  private ConnectionCoordinate(String connectionString, Authenticator authenticator, String username, Properties properties) {
    this.connectionString = notNullOrEmpty(connectionString, "ConnectionString");
    this.authenticator = notNull(authenticator, "Authenticator");
    this.properties = properties == null ? new Properties() : properties;
  }

  public String connectionString() {
    return connectionString;
  }

  public Authenticator authenticator() {
    return authenticator;
  }

  public Properties properties() {
    return properties;
  }

}
