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

import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.streampipes.dataexplorer.iotdb.v0.params.GroupingByTagsParams;
import org.apache.streampipes.dataexplorer.iotdb.v0.template.QueryTemplates;
import org.apache.streampipes.dataexplorer.iotdb.v0.utils.TagProvider;

import java.util.Map;

public class GroupingByTags extends QueryElement<GroupingByTagsParams> {

    public GroupingByTags(GroupingByTagsParams groupingByTagsParams) {
        super(groupingByTagsParams);
    }

    @Override
    protected String buildStatement(GroupingByTagsParams groupingByTagsParams) {
        String tags = "";
        String[] tagKeys;
        try {
            tagKeys = TagProvider.getTagKeys(groupingByTagsParams.getIndex());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException("select tagKeys error ");
        }
        int startLevel = 4;
        for (String tag : groupingByTagsParams.getGroupingTags()) {
            for (int i = 0; i < tagKeys.length; i++) {
                if (tagKeys[i].equals(tag)) {
                    tags = (startLevel + i) + ",";
                    break;
                }
            }
        }

        if(StringUtils.isBlank(tags)){
            throw new RuntimeException("select tags not found in database");
        }
        return QueryTemplates.groupByTags(tags.substring(0,tags.length()-1));
    }

}
