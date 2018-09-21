/*
 * Copyright 2018 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.streampipes.connect.adapter.specific;

import com.google.gson.Gson;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streampipes.connect.adapter.generic.pipeline.AdapterPipeline;
import org.streampipes.connect.adapter.generic.pipeline.AdapterPipelineElement;
import org.streampipes.connect.adapter.generic.pipeline.elements.SendToKafkaAdapterSink;
import org.streampipes.connect.adapter.generic.pipeline.elements.TransformSchemaAdapterPipelineElement;
import org.streampipes.connect.adapter.util.PollingSettings;
import org.streampipes.connect.exception.AdapterException;
import org.streampipes.model.connect.adapter.AdapterDescription;
import org.streampipes.model.connect.adapter.SpecificAdapterStreamDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class PullAdapter extends SpecificDataStreamAdapter {

    protected static Logger logger = LoggerFactory.getLogger(PullAdapter.class);
    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService errorThreadscheduler;

    protected AdapterPipeline adapterPipeline;

    public PullAdapter() {
        super();
    }

    public PullAdapter(SpecificAdapterStreamDescription adapterDescription) {
        super(adapterDescription);
    }

    protected abstract void pullData();

    protected abstract PollingSettings getPollingIntervalInSeconds();

    @Override
    public void startAdapter() throws AdapterException {

        List<AdapterPipelineElement> pipelineElements = new ArrayList<>();
        pipelineElements.add(new TransformSchemaAdapterPipelineElement(adapterDescription.getRules()));
        pipelineElements.add(new SendToKafkaAdapterSink((AdapterDescription) adapterDescription));

        adapterPipeline = new AdapterPipeline(pipelineElements);

        final Runnable errorThread = () -> {
            executeAdpaterLogic();
        };


        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(errorThread, 0, TimeUnit.MILLISECONDS);

    }

    private void executeAdpaterLogic() {
        final Runnable task = () -> {

            pullData();

        };

        scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> handle = scheduler.scheduleAtFixedRate(task, 1,
                getPollingIntervalInSeconds().getValue(), getPollingIntervalInSeconds().getTimeUnit());

        try {
            handle.get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error", e);
        }
    }

    @Override
    public void stopAdapter() throws AdapterException {
        scheduler.shutdownNow();
    }
}