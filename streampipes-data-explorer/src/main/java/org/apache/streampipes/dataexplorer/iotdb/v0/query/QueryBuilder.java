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

package org.apache.streampipes.dataexplorer.iotdb.v0.query;

import org.apache.streampipes.dataexplorer.iotdb.v0.query.elements.QueryElement;

import java.util.List;
import java.util.StringJoiner;

public class QueryBuilder {

  private final StringJoiner queryParts;
  private final String databaseName;

  private QueryBuilder(String databaseName) {
    this.queryParts = new StringJoiner(" ");
    this.databaseName = databaseName;
  }

  public static QueryBuilder create(String databaseName) {
    return new QueryBuilder(databaseName);
  }

  public String build(List<QueryElement<?>> queryElements, Boolean onlyCountResults) {
    for (QueryElement<?> queryPart : queryElements) {
      this.queryParts.add(queryPart.getStatement());
    }
    if (onlyCountResults) {
      return toCountResultsQuery();
    } else {
      return toQuery();
    }
  }

  public String toQuery() {
    return this.queryParts.toString();
  }

  //todo 子查询不确定对不对
  public String toCountResultsQuery() {
    return "SELECT COUNT(*) FROM " + this.queryParts.toString().substring(this.queryParts.toString().indexOf("FROM") + 4) + "";
  }
}



