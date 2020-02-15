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

package org.apache.streampipes.rest.api;


import org.apache.streampipes.model.client.pipeline.Pipeline;

import javax.ws.rs.core.Response;

public interface IPipeline extends IPipelineElement {

  Response addPipeline(String username, Pipeline pipeline);

  Response getSystemPipelines();

  Response start(String username, String pipelineId);

  Response stop(String username, String pipelineId);

  Response recommend(String email, Pipeline pipeline);

  Response update(Pipeline pipeline, String username);

  Response overwritePipeline(String username, String pipelineId, Pipeline pipeline);

  Response getPipelineStatus(String username, String pipelineId);
}