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

package org.apache.streampipes.dataexplorer.iotdb.v0.query.elements;

import org.apache.streampipes.commons.environment.Environment;
import org.apache.streampipes.commons.environment.Environments;
import org.apache.streampipes.dataexplorer.commons.configs.IotdbDbNameConfig;
import org.apache.streampipes.dataexplorer.iotdb.v0.params.DeleteFromStatementParams;
import org.apache.streampipes.dataexplorer.iotdb.v0.template.QueryTemplates;

public class DeleteFromStatement extends QueryElement<DeleteFromStatementParams> {
  public DeleteFromStatement(DeleteFromStatementParams deleteFromStatementParams) {
    super(deleteFromStatementParams);
  }

  @Override
  protected String buildStatement(DeleteFromStatementParams deleteFromStatementParams) {
    return QueryTemplates.deleteFrom(escapeIndex(deleteFromStatementParams.getIndex()));
  }

  private Environment getEnvironment() {
    return Environments.getEnvironment();
  }

  private String escapeIndex(String index) {
    //todo 是否去掉**
    return getEnvironment().getTsIotdbStorageBucket().getValueOrDefault() + ".`" + index + "`.**";
  }
}