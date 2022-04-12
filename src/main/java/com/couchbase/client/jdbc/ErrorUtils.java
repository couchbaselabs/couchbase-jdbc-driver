/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;

/**
 * Contains various error and exception utils for jdbc sql.
 */
public class ErrorUtils {

  private static final String AUTH_ERROR_CODE = "28000";

  private ErrorUtils() {}

  /**
   * Throws a SQL authentication error with the 28000 error code.
   *
   * @param cause the cause, can be null.
   * @return the exception that can be thrown.
   */
  public static SQLException authError(final Throwable cause) {
    return new SQLInvalidAuthorizationSpecException(
      "Authentication/authorization error - please check credentials.", AUTH_ERROR_CODE, cause
    );
  }

}
