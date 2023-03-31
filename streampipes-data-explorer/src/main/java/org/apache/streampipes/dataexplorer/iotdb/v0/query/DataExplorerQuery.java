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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.streampipes.commons.environment.Environment;
import org.apache.streampipes.commons.environment.Environments;
import org.apache.streampipes.dataexplorer.commons.iotdb.IotdbSessionProvider;
import org.apache.streampipes.dataexplorer.iotdb.v0.params.*;
import org.apache.streampipes.dataexplorer.iotdb.v0.query.elements.*;
import org.apache.streampipes.dataexplorer.iotdb.v0.utils.TagProvider;
import org.apache.streampipes.dataexplorer.iotdb.v0.utils.DataLakeManagementUtils;
import org.apache.streampipes.model.datalake.DataSeries;
import org.apache.streampipes.model.datalake.SpQueryResult;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataExplorerQuery {

    private static final Logger LOG = LoggerFactory.getLogger(DataExplorerQuery.class);

    protected Map<String, IotdbQueryParams> params;

    protected int maximumAmountOfEvents;

    private boolean appendId = false;
    private String forId;

    private Environment env;

    public DataExplorerQuery() {

    }

    public DataExplorerQuery(Map<String, IotdbQueryParams> params,
                             String forId) {
        this(params);
        this.appendId = true;
        this.forId = forId;
    }

    public DataExplorerQuery(Map<String, IotdbQueryParams> params) {
        this.params = params;
        this.env = Environments.getEnvironment();
        this.maximumAmountOfEvents = -1;
    }

    public DataExplorerQuery(Map<String, IotdbQueryParams> params, int maximumAmountOfEvents) {
        this(params);
        this.maximumAmountOfEvents = maximumAmountOfEvents;
    }

    public SpQueryResult executeQuery(boolean ignoreMissingValues) throws RuntimeException {
        SessionPool sessionPool = IotdbSessionProvider.getSessionPool();
        List<QueryElement<?>> queryElements = getQueryElements();

        //todo 不确定能不能实现
//        if (this.maximumAmountOfEvents != -1) {
//            QueryBuilder countQueryBuilder = QueryBuilder.create(getDatabaseName());
//            String query = countQueryBuilder.build(queryElements, true);
//            SessionDataSetWrapper sessionDataSetWrapper = null;
//            try {
//                sessionDataSetWrapper = sessionPool.executeQueryStatement(query);
//            } catch (IoTDBConnectionException | StatementExecutionException e) {
//                throw new RuntimeException(e);
//            }
//
//            Double amountOfQueryResults = getAmountOfResults(countQueryResult);
//
//            if (amountOfQueryResults > this.maximumAmountOfEvents) {
//                SpQueryResult tooMuchData = new SpQueryResult();
//                tooMuchData.setSpQueryStatus(SpQueryStatus.TOO_MUCH_DATA);
//                tooMuchData.setTotal(amountOfQueryResults.intValue());
//                return tooMuchData;
//            }
//        }

        QueryBuilder queryBuilder = QueryBuilder.create(getDatabaseName());

        String query = queryBuilder.build(queryElements, false);
//        if (LOG.isDebugEnabled()) {
            LOG.info("Data Lake Query (database:" + getDatabaseName() + "): " + query);
//        }

        SessionDataSetWrapper sessionDataSetWrapper = null;
        try {
            sessionDataSetWrapper = sessionPool.executeQueryStatement(query);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("Data Lake Query Result: " + sessionDataSetWrapper.toString());
        }

        return postQuery(sessionDataSetWrapper, ignoreMissingValues);

    }

    private double getAmountOfResults(QueryResult countQueryResult) {
        if (countQueryResult.getResults().get(0).getSeries() != null
                && countQueryResult.getResults().get(0).getSeries().get(0).getValues() != null) {
            return (double) countQueryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(1);
        } else {
            return 0.0;
        }
    }

    protected DataSeries convertResult(SessionDataSetWrapper sessionDataSetWrapper, boolean ignoreMissingValues) throws RuntimeException {
        try {

            List<String> columns = sessionDataSetWrapper.getColumnNames();
            List<String> columnTypes = sessionDataSetWrapper.getColumnTypes();
            List<List<Object>> values = new ArrayList<>();
            while (sessionDataSetWrapper.hasNext()) {
                boolean put = true;
                List<Object> list = new ArrayList<>();
                RowRecord rowRecord = sessionDataSetWrapper.next();
                list.add(rowRecord.getTimestamp());
                List<Field> fields = rowRecord.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    Object value = getValue(fields, columnTypes, i);
                    if (ignoreMissingValues) {
                        put = false;
                        break;
                    }
                    list.add(value);
                }
                if (!put) {
                    continue;
                }
                values.add(list);
            }
            //todo 源代码这个位置我觉得是个bug
            return new DataSeries(values.size(), values, columns, TagProvider.getTags(getDatabaseName()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getValue(List<Field> fields, List<String> columnTypes, int i) {
        String type = columnTypes.get(i + 1);
        //todo
        if ("null".equals(fields.get(i).getStringValue())) {
            return null;
        }
        if (TSDataType.INT32.name().equals(type)) {
            return fields.get(i).getIntV();
        } else if (TSDataType.INT64.name().equals(type)) {
            return fields.get(i).getLongV();
        } else if (TSDataType.BOOLEAN.name().equals(type)) {
            return fields.get(i).getBoolV();
        } else if (TSDataType.FLOAT.name().equals(type)) {
            return fields.get(i).getFloatV();
        } else if (TSDataType.DOUBLE.name().equals(type)) {
            return fields.get(i).getDoubleV();
        } else {
            return fields.get(i).getStringValue();
        }
    }


    protected SpQueryResult postQuery(SessionDataSetWrapper sessionDataSetWrapper,
                                      boolean ignoreMissingValues) throws RuntimeException {
        SpQueryResult result = new SpQueryResult();

        DataSeries dataSeries = convertResult(sessionDataSetWrapper, ignoreMissingValues);
        result.setHeaders(setHeader(dataSeries.getHeaders()));
        result.addDataResult(dataSeries);
        result.setTotal(dataSeries.getTotal());

        if (this.appendId) {
            result.setForId(this.forId);
        }

        return result;
    }

    private List<String> setHeader(List<String> list) {
        List<String> headers = new ArrayList<>();
        for (String column : list) {
            if ("Time".equals(column)) {
                headers.add("time");
                continue;
            }
            headers.add(column.substring(column.lastIndexOf(".") + 1));
        }
        return headers;
    }

    protected List<QueryElement<?>> getQueryElements() {
        List<QueryElement<?>> queryElements = new ArrayList<>();

        if (this.params.containsKey(DataLakeManagementUtils.SELECT_FROM)) {
            queryElements.add(
                    new SelectFromStatement((SelectFromStatementParams) this.params.get(DataLakeManagementUtils.SELECT_FROM)));
        } else {
            queryElements.add(
                    new DeleteFromStatement((DeleteFromStatementParams) this.params.get(DataLakeManagementUtils.DELETE_FROM)));
        }

        if (this.params.containsKey(DataLakeManagementUtils.WHERE)) {
            queryElements.add(new WhereStatement((WhereStatementParams) this.params.get(DataLakeManagementUtils.WHERE)));
        }

        //todo group by 参数拼接成语句
        if (this.params.containsKey(DataLakeManagementUtils.GROUP_BY_TIME)) {
            queryElements.add(
                    new GroupingByTime((GroupingByTimeParams) this.params.get(DataLakeManagementUtils.GROUP_BY_TIME)));

        } else if (this.params.containsKey(DataLakeManagementUtils.GROUP_BY_TAGS)) {
            queryElements.add(
                    new GroupingByTags((GroupingByTagsParams) this.params.get(DataLakeManagementUtils.GROUP_BY_TAGS)));
        }

        //todo fill 参数拼接成语句
        if (this.params.containsKey(DataLakeManagementUtils.FILL)) {
            queryElements.add(new FillStatement((FillParams) this.params.get(DataLakeManagementUtils.FILL)));
        }

        if (this.params.containsKey(DataLakeManagementUtils.ORDER_DESCENDING)) {
            queryElements.add(
                    new OrderingByTime((OrderingByTimeParams) this.params.get(DataLakeManagementUtils.ORDER_DESCENDING)));
        }
//        else if (this.params.containsKey(IotdbDataLakeManagementUtils.SELECT_FROM)) {
//            //todo 好像没用
//            queryElements.add(new OrderingByTime(
//                    OrderingByTimeParams.from(this.params.get(IotdbDataLakeManagementUtils.SELECT_FROM).getIndex(), "ASC")));
//        }

        if (this.params.containsKey(DataLakeManagementUtils.LIMIT)) {
            queryElements.add(new ItemLimitation((ItemLimitationParams) this.params.get(DataLakeManagementUtils.LIMIT)));
        }

        if (this.params.containsKey(DataLakeManagementUtils.OFFSET)) {
            queryElements.add(new Offset((OffsetParams) this.params.get(DataLakeManagementUtils.OFFSET)));
        }

        return queryElements;
    }

    private String getDatabaseName() {
        return env.getTsStorageBucket().getValueOrDefault();
    }


}
