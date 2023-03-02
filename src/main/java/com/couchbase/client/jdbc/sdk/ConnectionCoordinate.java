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

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A {@link ConnectionCoordinate} provides a "pointer" to a cluster managed by the SDK.
 */
public class ConnectionCoordinate {

  private final String connectionString;
  private final String username;
  private final String password;
  private final Authenticator authenticator;
  private final Properties properties;
  private final Duration connectTimeout;

  /**
   * Creates a new Connection Coordinate.
   *
   * @param connectionString the SDK connection string.
   * @param username the SDK username.
   * @param password the SDK password.
   * @param properties custom SDK properties.
   * @param connectTimeout the SDK connection timeout.
   * @return the created coordinate.
   */
  public static ConnectionCoordinate create(String connectionString, String username, String password,
                                            Properties properties, Duration connectTimeout) {
    return new ConnectionCoordinate(connectionString, username, password, properties, connectTimeout);
  }

  private ConnectionCoordinate(String connectionString, String username, String password, Properties properties,
                               Duration connectTimeout) {
    this.connectionString = notNullOrEmpty(connectionString, "ConnectionString");
    this.username = username;
    this.password = password;
    this.authenticator = notNull(PasswordAuthenticator.create(username, password), "Authenticator");
    this.properties = properties == null ? new Properties() : properties;
    this.connectTimeout = connectTimeout;
  }

  /**
   * Returns the connection string.
   *
   * @return the connection string.
   */
  public String connectionString() {
    return connectionString;
  }

  /**
   * The authenticator.
   *
   * @return the authenticator.
   */
  public Authenticator authenticator() {
    return authenticator;
  }

  /**
   * All current used properties.
   *
   * @return the used properties.
   */
  public Properties properties() {
    return properties;
  }

  /**
   * The used connect timeout.
   *
   * @return the used connect timeout.
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  @Override
  public String toString() {
    return "ConnectionCoordinate{" +
      "connectionString='" + connectionString + '\'' +
      ", authenticator=" + authenticator.getClass() +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConnectionCoordinate that = (ConnectionCoordinate) o;
    return Objects.equals(connectionString, that.connectionString) && Objects.equals(username, that.username)
      && Objects.equals(password, that.password) && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionString, username, password, properties);
  }
}
