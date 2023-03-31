package org.apache.streampipes.dataexplorer.iotdb.v0.query;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.streampipes.commons.environment.Environment;
import org.apache.streampipes.commons.environment.Environments;
import org.apache.streampipes.dataexplorer.commons.configs.IotdbDbNameConfig;
import org.apache.streampipes.dataexplorer.commons.iotdb.IotdbSessionProvider;
import org.apache.streampipes.model.datalake.DataLakeMeasure;

public class IotdbDeleteDataQuery {

    private final DataLakeMeasure measure;

    public IotdbDeleteDataQuery(DataLakeMeasure measure) {
        this.measure = measure;
    }

    public void deleteQuery() throws StatementExecutionException, IoTDBConnectionException {
        final SessionPool sessionPool = IotdbSessionProvider.getSessionPool();
        sessionPool.executeNonQueryStatement("delete timeseries " + getEnvironment().getTsIotdbStorageBucket().getValueOrDefault() + ".`" + measure.getMeasureName() + "`.**");
    }

    private static Environment getEnvironment() {
        return Environments.getEnvironment();
    }

}
