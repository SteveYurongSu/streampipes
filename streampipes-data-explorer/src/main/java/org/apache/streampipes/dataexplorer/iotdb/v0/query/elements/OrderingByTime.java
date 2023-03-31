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

import org.apache.streampipes.dataexplorer.iotdb.v0.params.OrderingByTimeParams;
import org.apache.streampipes.dataexplorer.iotdb.v0.template.QueryTemplates;

public class OrderingByTime extends QueryElement<OrderingByTimeParams> {

  public OrderingByTime(OrderingByTimeParams orderingByTimeParams) {
    super(orderingByTimeParams);
  }

  @Override
  protected String buildStatement(OrderingByTimeParams orderingByTimeParams) {
    return QueryTemplates.orderByTime(orderingByTimeParams.getOrdering());
  }
}