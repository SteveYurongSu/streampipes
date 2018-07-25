/*
Copyright 2018 FZI Forschungszentrum Informatik

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.streampipes.connect.firstconnector.protocol.stream;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.fluent.Request;
import org.streampipes.connect.firstconnector.format.Format;
import org.streampipes.connect.firstconnector.format.Parser;
import org.streampipes.connect.firstconnector.guess.SchemaGuesser;
import org.streampipes.connect.firstconnector.protocol.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streampipes.connect.firstconnector.sdk.ParameterExtractor;
import org.streampipes.model.modelconnect.GuessSchema;
import org.streampipes.model.modelconnect.ProtocolDescription;
import org.streampipes.model.schema.EventSchema;
import org.streampipes.model.staticproperty.FreeTextStaticProperty;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpStreamProtocol extends PullProtocoll {

    Logger LOG = LoggerFactory.getLogger(HttpStreamProtocol.class);

    public static String ID = "https://streampipes.org/vocabulary/v1/protocol/stream/http";
    private static String URL_PROPERTY ="url";
    private static String INTERVAL_PROPERTY ="interval";

    private String url;

    public HttpStreamProtocol() {
    }



    public HttpStreamProtocol(Parser parser, Format format, String url, long interval) {
        super(parser, format, interval);
        this.url = url;
    }

    @Override
    public Protocol getInstance(ProtocolDescription protocolDescription, Parser parser, Format format) {
        ParameterExtractor extractor = new ParameterExtractor(protocolDescription.getConfig());

        String urlProperty = extractor.singleValue(URL_PROPERTY);
        try {
            long intervalProperty = Long.parseLong(extractor.singleValue(INTERVAL_PROPERTY));
            return new HttpStreamProtocol(parser, format, urlProperty, intervalProperty);
        } catch (NumberFormatException e) {
            LOG.error("Could not parse" + extractor.singleValue(INTERVAL_PROPERTY) + "to int");
            return null;
        }

    }

    @Override
    public ProtocolDescription declareModel() {
        ProtocolDescription description = new ProtocolDescription(ID, "HTTP (Stream)", "This is the " +
                "description for the File protocol");

        FreeTextStaticProperty urlProperty = new FreeTextStaticProperty(URL_PROPERTY, "URL", "This property " +
                "defines the URL for the http request.");


        FreeTextStaticProperty intervalProperty = new FreeTextStaticProperty(INTERVAL_PROPERTY, "Interval [Sec]", "This property " +
                "defines the pull interval in seconds.");


        description.setSourceType("STREAM");
        description.addConfig(urlProperty);
        description.addConfig(intervalProperty);

        return description;
    }

    @Override
    public GuessSchema getGuessSchema() {
        int n = 20;

        InputStream dataInputStream = getDataFromEndpoint();

        List<byte[]> dataByte = parser.parseNEvents(dataInputStream, n);
        if (dataByte.size() < n) {
            LOG.error("Error in HttpStreamProtocol! Required: " + n + " elements but the resource just had: " +
                    dataByte.size());

            dataByte.addAll(dataByte);
        }
        EventSchema eventSchema= parser.getEventSchema(dataByte);
        GuessSchema result = SchemaGuesser.guessSchma(eventSchema, getNElements(n));

        return result;
    }

    @Override
    public List<Map<String, Object>> getNElements(int n) {
        List<Map<String, Object>> result = new ArrayList<>();

        InputStream dataInputStream = getDataFromEndpoint();

        List<byte[]> dataByte = parser.parseNEvents(dataInputStream, n);

        // Check that result size is n. Currently just an error is logged. Maybe change to an exception
        if (dataByte.size() < n) {
            LOG.error("Error in HttpStreamProtocol! User required: " + n + " elements but the resource just had: " +
                    dataByte.size());
        }

        for (byte[] b : dataByte) {
            result.add(format.parse(b));
        }

        return result;
    }


    @Override
    public String getId() {
        return ID;
    }

    @Override
    InputStream getDataFromEndpoint() {
        InputStream result = null;

        try {
            String s = Request.Get(url)
                    .connectTimeout(1000)
                    .socketTimeout(100000)
                    .execute().returnContent().asString();

            if (s.startsWith("ï")) {
                s = s.substring(3);
            }

            result = IOUtils.toInputStream(s, "UTF-8");

        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }
}