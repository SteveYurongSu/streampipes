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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.streampipes.commons.environment.Environment;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.dataexplorer.commons.configs.IotdbDbNameConfig;
import org.apache.streampipes.model.datalake.DataLakeMeasure;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.runtime.field.PrimitiveField;
import org.apache.streampipes.model.schema.EventProperty;
import org.apache.streampipes.model.schema.EventPropertyPrimitive;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.vocabulary.XSD;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IotdbStore {

    private static final Logger LOG = LoggerFactory.getLogger(IotdbStore.class);
    DataLakeMeasure measure;
    Map<String, String> sanitizedRuntimeNames = new HashMap<>();
    private SessionPool sessionPool = null;
    private String deviceName;
    private String saveDatabaseName;

    public IotdbStore(DataLakeMeasure measure,
                      IotdbConnectionSettings settings) {
        this.measure = measure;
        // store sanitized target property runtime names in local variable
        measure.getEventSchema()
                .getEventProperties()
                .forEach(ep -> sanitizedRuntimeNames.put(ep.getRuntimeName(),
                        IotdbNameSanitizer.renameReservedKeywords(ep.getRuntimeName())));

        connect(settings);
    }

    public IotdbStore(DataLakeMeasure measure,
                      Environment environment) throws SpRuntimeException {
        this(measure, IotdbConnectionSettings.from(environment));
    }

    /**
     * Connects to the InfluxDB Server, sets the database and initializes the batch-behaviour
     *
     * @throws SpRuntimeException If not connection can be established or if the database could not
     *                            be found
     */
    private void connect(IotdbConnectionSettings settings) throws SpRuntimeException {
        try {
            this.sessionPool = IotdbSessionProvider.getSessionPool(settings);
        } catch (Exception e) {
            throw new SpRuntimeException("Could not connect to InfluxDb Server: " + settings.getIotdbDbHost() + ":" + settings.getIotdbPort());
        }
        deviceName = measure.getMeasureName();
        saveDatabaseName = settings.getDatabaseName() + ".`" + deviceName + "`";
    }

    /**
     * Saves an event to the connected iotdb database
     *
     * @param event The event which should be saved
     * @throws SpRuntimeException If the column name (key-value of the event map) is not allowed
     */
    public void onEvent(Event event) throws SpRuntimeException {
        var missingFields = new ArrayList<String>();
        var nullFields = new ArrayList<String>();

        Map<String, Integer> deviceTagMap = new HashMap<>();
        Map<Integer, String> deviceMap = new HashMap<>();
        List<String> measurementsList = new ArrayList<>();
        List<TSDataType> typesList = new ArrayList<>();
        List<Object> valuesList = new ArrayList<>();

        if (event == null) {
            throw new SpRuntimeException("event is null");
        }

        Long timestampValue = event.getFieldBySelector(measure.getTimestampField()).getAsPrimitive().getAsLong();

        try {
            String tagNameSort = getTagNameSort();
            if (StringUtils.isNotBlank(tagNameSort)) {
                String[] split = tagNameSort.split(",");
                for (int i = 0; i < split.length; i++) {
                    deviceTagMap.put(split[i], i);
                }
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new SpRuntimeException(e);
        }

        for (EventProperty ep : measure.getEventSchema().getEventProperties()) {
            if (ep instanceof EventPropertyPrimitive) {
                String runtimeName = ep.getRuntimeName();

                // timestamp should not be added as a field
                if (!measure.getTimestampField().endsWith(runtimeName)) {
                    String sanitizedRuntimeName = sanitizedRuntimeNames.get(runtimeName);

                    try {
                        var field = event.getOptionalFieldByRuntimeName(runtimeName);
                        if (field.isPresent()) {
                            PrimitiveField eventPropertyPrimitiveField = field.get().getAsPrimitive();
                            if (eventPropertyPrimitiveField.getRawValue() == null) {
                                nullFields.add(sanitizedRuntimeName);
                            } else {

                                // store property as tag when the field is a dimension property
                                if (PropertyScope.DIMENSION_PROPERTY.name().equals(ep.getPropertyScope())) {
                                    Integer integer = deviceTagMap.get(runtimeName);
                                    if (null != integer) {
                                        deviceMap.put(integer, eventPropertyPrimitiveField.getAsString());
                                    }
                                } else {
                                    handleMeasurementProperty(
                                            measurementsList,
                                            typesList,
                                            valuesList,
                                            (EventPropertyPrimitive) ep,
                                            sanitizedRuntimeName,
                                            eventPropertyPrimitiveField);
                                }
                            }
                        } else {
                            missingFields.add(runtimeName);
                        }
                    } catch (SpRuntimeException iae) {
                        LOG.warn("Runtime exception while extracting field value of field {} - this field will be ignored",
                                runtimeName, iae);
                    }
                }
            }
        }

        if (!missingFields.isEmpty()) {
            LOG.debug("Ignored {} fields which were present in the schema, but not in the provided event: {}",
                    missingFields.size(),
                    String.join(", ", missingFields));
        }

        if (!nullFields.isEmpty()) {
            LOG.warn("Ignored {} fields which had a value 'null': {}", nullFields.size(), String.join(", ", nullFields));
        }

        if (deviceTagMap.size() != deviceMap.size()) {
            throw new SpRuntimeException("device length is error, please make sure eventSchema");
        }

        try {
            this.sessionPool.insertRecord(saveDatabaseName + getDeviceId(deviceMap), timestampValue, measurementsList, typesList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new SpRuntimeException(e);
        }
    }

    private String getDeviceId(Map<Integer, String> deviceMap) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < deviceMap.size(); i++) {
            sb.append(".`" + deviceMap.get(i) + "`");
        }
        return sb.toString();
    }

    private String getTagNameSort() throws IoTDBConnectionException, StatementExecutionException {


        SessionDataSetWrapper sessionDataSetWrapper = sessionPool.executeQueryStatement("select `" + deviceName + "` from " + IotdbDbNameConfig.DATABASE_TAG_DB_NAME_PREFIX);
        if (sessionDataSetWrapper.hasNext()) {
            return sessionDataSetWrapper.next().getFields().get(0).getStringValue();
        } else {
            String sortDevice = "";
            List<String> devices = new ArrayList<>();

            for (EventProperty ep : measure.getEventSchema().getEventProperties()) {
                if (PropertyScope.DIMENSION_PROPERTY.name().equals(ep.getPropertyScope())) {
                    devices.add(ep.getRuntimeName());
                }
            }

            if (!CollectionUtils.isEmpty(devices)) {
                Collections.sort(devices);
                sortDevice = String.join(",", devices);
            }

            this.sessionPool.insertRecord(IotdbDbNameConfig.DATABASE_TAG_DB_NAME_PREFIX,
                    System.currentTimeMillis(),
                    new ArrayList<>(Collections.singleton("`" + deviceName + "`")),
                    new ArrayList<>(Collections.singleton(TSDataType.TEXT)),
                    new ArrayList<>(Collections.singleton(sortDevice)));
            return sortDevice;
        }
    }

    private void handleMeasurementProperty(List<String> measurementsList,
                                           List<TSDataType> typesList,
                                           List<Object> valuesList,
                                           @NotNull EventPropertyPrimitive ep,
                                           String preparedRuntimeName,
                                           PrimitiveField eventPropertyPrimitiveField) {
        try {
            // Store property according to property type
            String runtimeType = ep.getRuntimeType();
            measurementsList.add(preparedRuntimeName);
            if (XSD.INTEGER.toString().equals(runtimeType)) {
                try {
                    typesList.add(TSDataType.INT32);
                    valuesList.add(eventPropertyPrimitiveField.getAsInt());
                } catch (NumberFormatException ef) {
                    typesList.add(TSDataType.FLOAT);
                    valuesList.add(eventPropertyPrimitiveField.getAsFloat());
                }
            } else if (XSD.LONG.toString().equals(runtimeType)) {
                try {
                    typesList.add(TSDataType.INT64);
                    valuesList.add(eventPropertyPrimitiveField.getAsLong());
                } catch (NumberFormatException ef) {
                    typesList.add(TSDataType.FLOAT);
                    valuesList.add(eventPropertyPrimitiveField.getAsFloat());
                }
            } else if (XSD.FLOAT.toString().equals(runtimeType)) {
                typesList.add(TSDataType.FLOAT);
                valuesList.add(eventPropertyPrimitiveField.getAsFloat());
            } else if (XSD.DOUBLE.toString().equals(runtimeType)) {
                typesList.add(TSDataType.DOUBLE);
                valuesList.add(eventPropertyPrimitiveField.getAsDouble());
            } else if (XSD.BOOLEAN.toString().equals(runtimeType)) {
                typesList.add(TSDataType.BOOLEAN);
                valuesList.add(eventPropertyPrimitiveField.getAsBoolean());
            } else if (SO.NUMBER.equals(runtimeType)) {
                typesList.add(TSDataType.DOUBLE);
                valuesList.add(eventPropertyPrimitiveField.getAsDouble());
            } else {
                typesList.add(TSDataType.TEXT);
                valuesList.add(eventPropertyPrimitiveField.getAsString());
            }
        } catch (NumberFormatException e) {
            LOG.warn("Wrong number format for field {}, ignoring.", preparedRuntimeName);
        }
    }

    /**
     * Shuts down the connection to the InfluxDB server
     */
    public void close() throws SpRuntimeException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new SpRuntimeException(e);
        }
        sessionPool.close();
    }
}
