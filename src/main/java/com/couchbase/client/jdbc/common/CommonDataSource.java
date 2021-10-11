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

package com.couchbase.client.jdbc.common;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public abstract class CommonDataSource implements DataSource {

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    // TODO: wire up
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    // TODO: wire up
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    // TODO: wire up
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    // TODO: wire up
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO: wire up
    return null;
  }

}
