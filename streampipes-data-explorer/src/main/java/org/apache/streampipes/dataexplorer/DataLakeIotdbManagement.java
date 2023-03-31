package org.apache.streampipes.dataexplorer;

import com.google.gson.JsonObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.streampipes.commons.environment.Environment;
import org.apache.streampipes.commons.environment.Environments;
import org.apache.streampipes.dataexplorer.iotdb.v0.ProvidedQueryParams;
import org.apache.streampipes.dataexplorer.iotdb.v0.params.IotdbQueryParams;
import org.apache.streampipes.dataexplorer.iotdb.v0.query.DataExplorerQuery;
import org.apache.streampipes.dataexplorer.iotdb.v0.query.IotdbDeleteDataQuery;
import org.apache.streampipes.dataexplorer.iotdb.v0.query.QueryResultProvider;
import org.apache.streampipes.dataexplorer.iotdb.v0.query.StreamedQueryResultProvider;
import org.apache.streampipes.dataexplorer.iotdb.v0.query.writer.OutputFormat;
import org.apache.streampipes.dataexplorer.iotdb.v0.utils.TagProvider;
import org.apache.streampipes.dataexplorer.iotdb.v0.utils.DataLakeManagementUtils;
import org.apache.streampipes.dataexplorer.utils.DataExplorerUtils;
import org.apache.streampipes.model.datalake.DataLakeMeasure;
import org.apache.streampipes.model.datalake.SpQueryResult;
import org.apache.streampipes.model.schema.EventProperty;
import org.apache.streampipes.model.schema.EventPropertyList;
import org.apache.streampipes.model.schema.EventPropertyNested;
import org.apache.streampipes.model.schema.EventPropertyPrimitive;
import org.apache.streampipes.storage.api.IDataLakeStorage;
import org.apache.streampipes.storage.couchdb.utils.Utils;
import org.apache.streampipes.storage.management.StorageDispatcher;
import org.lightcouch.CouchDbClient;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class DataLakeIotdbManagement {


    public List<DataLakeMeasure> getAllMeasurements() {
        return DataExplorerUtils.getInfos();
    }

    public DataLakeMeasure getById(String measureId) {
        return getDataLakeStorage().findOne(measureId);
    }

    public SpQueryResult getData(ProvidedQueryParams queryParams,
                                 boolean ignoreMissingData) throws IllegalArgumentException {
        return new QueryResultProvider(queryParams, ignoreMissingData).getData();
    }

    public void getDataAsStream(ProvidedQueryParams params,
                                OutputFormat format,
                                boolean ignoreMissingValues,
                                OutputStream outputStream) throws IOException {

        new StreamedQueryResultProvider(params, format, ignoreMissingValues).getDataAsStream(outputStream);
    }

    public boolean removeAllMeasurements() {
        List<DataLakeMeasure> allMeasurements = getAllMeasurements();
        for (DataLakeMeasure measure : allMeasurements) {
            try {
                new IotdbDeleteDataQuery(measure).deleteQuery();
            } catch (StatementExecutionException | IoTDBConnectionException e) {
                return false;
            }
        }
        return true;
    }

    public boolean removeMeasurement(String measurementID) {
        List<DataLakeMeasure> allMeasurements = getAllMeasurements();
        for (DataLakeMeasure measure : allMeasurements) {
            if (measure.getMeasureName().equals(measurementID)) {
                try {
                    new IotdbDeleteDataQuery(new DataLakeMeasure(measurementID, null)).deleteQuery();
                    return true;
                } catch (StatementExecutionException | IoTDBConnectionException e) {
                    return false;
                }
            }
        }
        return false;
    }

    public SpQueryResult deleteData(String measurementID, Long startDate, Long endDate) {
        Map<String, IotdbQueryParams> queryParts =
                DataLakeManagementUtils.getDeleteQueryParams(measurementID, startDate, endDate);
        return new DataExplorerQuery(queryParts).executeQuery(true);
    }

    public boolean removeEventProperty(String measurementID) {
        boolean isSuccess = false;
        CouchDbClient couchDbClient = Utils.getCouchDbDataLakeClient();
        List<JsonObject> docs = couchDbClient.view("_all_docs").includeDocs(true).query(JsonObject.class);

        for (JsonObject document : docs) {
            if (document.get("measureName").toString().replace("\"", "").equals(measurementID)) {
                couchDbClient.remove(document.get("_id").toString().replace("\"", ""),
                        document.get("_rev").toString().replace("\"", ""));
                isSuccess = true;
                break;
            }
        }

        try {
            couchDbClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isSuccess;
    }

    public Map<String, Object> getTagValues(String measurementId,
                                            String fields) {
        Map<String, Object> tags = new HashMap<>();
        if (fields != null && !("".equals(fields))) {
            List<String> fieldList = Arrays.asList(fields.split(","));
            try {
                String[] tagKeys = TagProvider.getTagKeys(measurementId);
                if (null == tagKeys) {
                    return tags;
                }
                List<String[]> tagValues = TagProvider.getTagValues(measurementId, tagKeys.length);
                if (CollectionUtils.isEmpty(tagValues)) {
                    return tags;
                }
                for (int i = 0; i < tagKeys.length; i++) {
                    int index = i;
                    Set<String> valueSet = tagValues.stream().filter(value->fieldList.contains(value[index])).map(value -> value[index]).collect(Collectors.toSet());
                    if(!valueSet.isEmpty()) {
                        tags.put(tagKeys[i], valueSet);
                    }
                }
                return tags;
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException("select tag error");
            }
        }
        return tags;

    }



    public void updateDataLake(DataLakeMeasure measure) throws IllegalArgumentException {
        var existingMeasure = getDataLakeStorage().findOne(measure.getElementId());
        if (existingMeasure != null) {
            measure.setRev(existingMeasure.getRev());
            getDataLakeStorage().updateDataLakeMeasure(measure);
        } else {
            getDataLakeStorage().storeDataLakeMeasure(measure);
        }
    }

    public void deleteDataLakeMeasure(String elementId) throws IllegalArgumentException {
        if (getDataLakeStorage().findOne(elementId) != null) {
            getDataLakeStorage().deleteDataLakeMeasure(elementId);
        } else {
            throw new IllegalArgumentException("Could not find measure with this ID");
        }
    }

    public DataLakeMeasure addDataLake(DataLakeMeasure measure) {
        List<DataLakeMeasure> dataLakeMeasureList = getDataLakeStorage().getAllDataLakeMeasures();
        Optional<DataLakeMeasure> optional =
                dataLakeMeasureList.stream().filter(entry -> entry.getMeasureName().equals(measure.getMeasureName()))
                        .findFirst();

        if (optional.isPresent()) {
            DataLakeMeasure oldEntry = optional.get();
            if (!compareEventProperties(oldEntry.getEventSchema().getEventProperties(),
                    measure.getEventSchema().getEventProperties())) {
                return oldEntry;
            }
        } else {
            measure.setSchemaVersion(DataLakeMeasure.CURRENT_SCHEMA_VERSION);
            getDataLakeStorage().storeDataLakeMeasure(measure);
            return measure;
        }

        return measure;
    }

    private boolean compareEventProperties(List<EventProperty> prop1, List<EventProperty> prop2) {
        if (prop1.size() != prop2.size()) {
            return false;
        }

        return prop1.stream().allMatch(prop -> {

            for (EventProperty property : prop2) {
                if (prop.getRuntimeName().equals(property.getRuntimeName())) {

                    //primitive
                    if (prop instanceof EventPropertyPrimitive && property instanceof EventPropertyPrimitive) {
                        if (((EventPropertyPrimitive) prop)
                                .getRuntimeType()
                                .equals(((EventPropertyPrimitive) property).getRuntimeType())) {
                            return true;
                        }

                        //list
                    } else if (prop instanceof EventPropertyList && property instanceof EventPropertyList) {
                        return compareEventProperties(Collections.singletonList(((EventPropertyList) prop).getEventProperty()),
                                Collections.singletonList(((EventPropertyList) property).getEventProperty()));

                        //nested
                    } else if (prop instanceof EventPropertyNested && property instanceof EventPropertyNested) {
                        return compareEventProperties(((EventPropertyNested) prop).getEventProperties(),
                                ((EventPropertyNested) property).getEventProperties());
                    }
                }
            }
            return false;

        });
    }


    private IDataLakeStorage getDataLakeStorage() {
        return StorageDispatcher.INSTANCE.getNoSqlStore().getDataLakeStorage();
    }

    private Environment getEnvironment() {
        return Environments.getEnvironment();
    }

}
