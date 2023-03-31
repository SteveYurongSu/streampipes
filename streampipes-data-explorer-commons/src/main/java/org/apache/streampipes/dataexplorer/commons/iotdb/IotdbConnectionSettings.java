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

import org.apache.streampipes.commons.environment.Environment;

public class IotdbConnectionSettings {

  private final Integer iotdbPort;
  private final String iotdbDbHost;
  private final String databaseName;

  private String username;
  private String password;

  private IotdbAuthMode authMode;


  private IotdbConnectionSettings(String iotdbDbHost,
                                  Integer iotdbPort,
                                  String databaseName) {
    this.iotdbDbHost = iotdbDbHost;
    this.iotdbPort = iotdbPort;
    this.databaseName = databaseName;
  }

  private IotdbConnectionSettings(String iotdbDbHost,
                                  Integer iotdbPort,
                                  String databaseName,
                                  String username,
                                  String password) {
    this(iotdbDbHost, iotdbPort, databaseName);
    this.username = username;
    this.password = password;
    this.authMode = IotdbAuthMode.USERNAME_PASSWORD;
  }

  public static IotdbConnectionSettings from(String iotdbDbHost,
                                             int iotdbPort,
                                             String databaseName,
                                             String username,
                                             String password) {
    return new IotdbConnectionSettings(iotdbDbHost, iotdbPort, databaseName, username, password);
  }


  public static IotdbConnectionSettings from(Environment environment) {

    return new IotdbConnectionSettings(
            environment.getTsIotdbStorageHost().getValueOrDefault(),
            environment.getTsIotdbStoragePort().getValueOrDefault(),
            environment.getTsIotdbStorageBucket().getValueOrDefault(),
            environment.getTsIotdbStorageUserName().getValueOrDefault(),
            environment.getTsIotdbStoragePassword().getValueOrDefault()
    );
  }

  public Integer getIotdbPort() {
    return iotdbPort;
  }

  public String getIotdbDbHost() {
    return iotdbDbHost;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public IotdbAuthMode getAuthMode() {
    return authMode;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
