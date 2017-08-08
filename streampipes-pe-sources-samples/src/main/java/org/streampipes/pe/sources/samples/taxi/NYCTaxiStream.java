package org.streampipes.pe.sources.samples.taxi;

import org.streampipes.model.impl.EventGrounding;
import org.streampipes.model.impl.EventSchema;
import org.streampipes.model.impl.EventStream;
import org.streampipes.model.impl.TransportFormat;
import org.streampipes.model.impl.graph.SepDescription;
import org.streampipes.model.vocabulary.MessageFormat;
import org.streampipes.pe.sources.samples.config.ProaSenseSettings;
import org.streampipes.pe.sources.samples.config.SourcesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streampipes.commons.Utils;

import java.io.File;

public class NYCTaxiStream extends AbstractNycStream {
	
	public static final Logger logger = LoggerFactory.getLogger(NYCTaxiStream.class);

	public NYCTaxiStream() {
		super(NycSettings.sampleTopic);
	}
	
	@Override
	public EventStream declareModel(SepDescription sep) {
			
		EventStream stream = new EventStream();
		stream.setIconUrl(SourcesConfig.iconBaseUrl + "/Taxi_Icon_2" +"_HQ.png");
		EventSchema schema = NycTaxiUtils.getEventSchema();

		EventGrounding grounding = new EventGrounding();
		grounding.setTransportProtocol(ProaSenseSettings.standardProtocol(NycSettings.sampleTopic));
		grounding.setTransportFormats(Utils.createList(new TransportFormat(MessageFormat.Json)));
		
		stream.setEventGrounding(grounding);
		stream.setEventSchema(schema);
		stream.setName("NYC Taxi Sample Stream");
		stream.setDescription("NYC Taxi Sample Stream Description");
		stream.setUri("taxi-sample");

		return stream;
	}

	@Override
	public void executeStream() {	
		File file = new File(NycSettings.completeDatasetFilename);
		executeReplay(file);
	}

	@Override
	public boolean isExecutable() {
		return true;
	}
	

	/**
	 * Sending
	 */
	class OutputThread implements Runnable {
		long diff;

		public OutputThread(long sleepTime) {
			diff = sleepTime;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(diff);
				synchronized (publisher) {
					//publisher.sendText(json);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

}