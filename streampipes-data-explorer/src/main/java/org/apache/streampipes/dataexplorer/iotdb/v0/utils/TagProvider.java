package org.apache.streampipes.dataexplorer.iotdb.v0.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.streampipes.commons.environment.Environment;
import org.apache.streampipes.commons.environment.Environments;
import org.apache.streampipes.dataexplorer.commons.configs.IotdbDbNameConfig;
import org.apache.streampipes.dataexplorer.commons.iotdb.IotdbSessionProvider;

import java.util.*;
import java.util.stream.Collectors;

public class TagProvider {

    public static String[] getTagKeys(String measurementId) throws IoTDBConnectionException, StatementExecutionException {
        SessionPool sessionPool = IotdbSessionProvider.getSessionPool();
        SessionDataSetWrapper sessionDataSetWrapper = sessionPool.executeQueryStatement("select `" + measurementId + "` from " + IotdbDbNameConfig.DATABASE_TAG_DB_NAME_PREFIX);
        if (sessionDataSetWrapper.hasNext()) {
            String tagKeys = sessionDataSetWrapper.next().getFields().get(0).getStringValue();
            if (StringUtils.isNotBlank(tagKeys)) {
                return tagKeys.split(",");
            }
        }
        return null;
    }

    public static List<String[]> getTagValues(String measurementId, int length) throws IoTDBConnectionException, StatementExecutionException {
        SessionPool sessionPool = IotdbSessionProvider.getSessionPool();
        String path = getEnvironment().getTsIotdbStorageBucket().getValueOrDefault() + ".`" + measurementId + "`.";
        SessionDataSetWrapper sessionDataSetWrapper = sessionPool.executeQueryStatement("show devices " + path + "**");
        List<String[]> list = new ArrayList<>();
        if (sessionDataSetWrapper.hasNext()) {
            String tagValue = sessionDataSetWrapper.next().getFields().get(0).getStringValue();
            if (StringUtils.isNotBlank(tagValue)) {
                String value = tagValue.substring(tagValue.indexOf(path) + path.length());
                String[] values = value.split("\\.");
                if (values.length == length) {
                    list.add(values);
                }
            }
        }

        return list;
    }

    public static Map<String, String> getTags(String measurementId) {
        try {
            Map<String, String> tags = new HashMap<>();
            String[] tagKeys = getTagKeys(measurementId);
            if (null == tagKeys) {
                return tags;
            }
            List<String[]> tagValues = getTagValues(measurementId, tagKeys.length);
            if (CollectionUtils.isEmpty(tagValues)) {
                return tags;
            }
            for (int i = 0; i < tagKeys.length; i++) {
                int index = i;
                Set<String> valueSet = tagValues.stream().map(value -> value[index]).collect(Collectors.toSet());
                if (!valueSet.isEmpty()) {
                    valueSet.forEach(value -> tags.put(tagKeys[index], value));
                }
            }
            return tags;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException("select tag error");
        }
    }

    private static Environment getEnvironment() {
        return Environments.getEnvironment();
    }
}
