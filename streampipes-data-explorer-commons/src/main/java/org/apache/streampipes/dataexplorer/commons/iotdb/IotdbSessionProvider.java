/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.dataexplorer.commons.iotdb;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.streampipes.commons.environment.Environment;
import org.apache.streampipes.commons.environment.Environments;

public class IotdbSessionProvider {

  /**
   * Create a new InfluxDB client from environment variables
   *
   * @return InfluxDB
   */
  public static SessionPool getSessionPool() {
    var env = getEnvironment();
    return getSessionPool(IotdbConnectionSettings.from(env));
  }

  /**
   * Create a new InfluxDB client from provided settings
   *
   * @param settings Connection settings
   * @return InfluxDB
   */
  public static SessionPool getSessionPool(IotdbConnectionSettings settings) {

      return new SessionPool.Builder()
              .host(settings.getIotdbDbHost())
              .port(settings.getIotdbPort())
              .user(settings.getUsername())
              .password(settings.getPassword())
              .connectionTimeoutInMs(3000)
              .fetchSize(10)
              .maxSize(100)
              .build();

  }

  private static Environment getEnvironment() {
    return Environments.getEnvironment();
  }
}
