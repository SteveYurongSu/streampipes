package de.fzi.cep.sepa.commons.config;

import java.io.*;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.JSONLDMode;
import org.openrdf.rio.helpers.JSONLDSettings;

/**
 * Configuration objects containing loads all properties from a file
 * and holds runtime changes.
 */
public class Configuration {

	private static Configuration instance;
	private PropertiesConfiguration config;
	
	public RDFFormat RDF_FORMAT = RDFFormat.JSONLD;
	
	public String COUCHDB_USER_DB;
	public String COUCHDB_PIPELINE_DB;
	public String COUCHDB_MONITORING_DB;
	public String COUCHDB_CONNECTION_DB; 
	public String COUCHDB_PROTOCOL;
	public String COUCHDB_HOSTNAME;
	public int COUCHDB_PORT = 5984;
	
	public String SESAME_URI;
	public String SESAME_REPOSITORY_ID;
	
	private String HOSTNAME;
	public  String SERVER_URL;
	
	public int ACTION_PORT;
	public int ESPER_PORT;
	public int ALGORITHM_PORT;
	public int SOURCES_PORT;
	
	public String ALGORITHM_BASE_URL = SERVER_URL +":" +ALGORITHM_PORT;
	public String ESPER_BASE_URL = SERVER_URL + ":" + ESPER_PORT;
	public String ACTION_BASE_URL = SERVER_URL + ":" + ACTION_PORT;
	public  String SOURCES_BASE_URL = SERVER_URL + ":" + SOURCES_PORT;
	
	public String CONTEXT_PATH = "/semantic-epa-backend";
	
	public String KAFKA_HOST;
	public String KAFKA_PROTOCOL;
	public int KAFKA_PORT;
	
	public String JMS_HOST;
	public String JMS_PROTOCOL;
	public int JMS_PORT;
	
	public  String TCP_SERVER_URL;
	public int TCP_SERVER_PORT;
	
	public int WEBAPP_PORT;
	public String WEBAPP_BASE_URL;
	
	public String ZOOKEEPER_HOST;
	public int ZOOKEEPER_PORT;
	public String ZOOKEEPER_PROTOCOL;
	
	public String HIPPO_URL;
	public String PANDDA_URL;
	public String STREAMSTORY_URL;	
	
	/**
	 * Constructor loads config data from config file.
	 */
	private Configuration() {
		
			if (ConfigurationManager.isConfigured())
			{
				Logger.getAnonymousLogger().info("Loading config file...");
				try {
					config = new PropertiesConfiguration(ConfigurationManager.getStreamPipesConfigFullPath());
					
					HOSTNAME = config.getString("hostname");
					SERVER_URL = config.getString("server_url");
					TCP_SERVER_URL = config.getString("tcp_server_url");
					TCP_SERVER_PORT = config.getInt("tcp_server_port")	;
					ACTION_PORT = config.getInt("action_port");
					ESPER_PORT = config.getInt("esper_port");
					ALGORITHM_PORT = config.getInt("algorithm_port");
					SOURCES_PORT = config.getInt("sources_port");
					WEBAPP_PORT = config.getInt("webapp_port");
					ALGORITHM_BASE_URL = SERVER_URL +":" +ALGORITHM_PORT;
					ESPER_BASE_URL = SERVER_URL + ":" + ESPER_PORT;
					ACTION_BASE_URL = SERVER_URL + ":" + ACTION_PORT;
					SOURCES_BASE_URL = SERVER_URL + ":" + SOURCES_PORT;
					WEBAPP_BASE_URL = SERVER_URL + ":" + WEBAPP_PORT;
					SESAME_URI = config.getString("sesameUrl");
					
					COUCHDB_PROTOCOL = config.getString("couchDbProtocol");
					COUCHDB_HOSTNAME = config.getString("couchDbHost");
					COUCHDB_PORT = config.getInt("couchDbPort");
					
					COUCHDB_USER_DB = config.getString("couchDbUserDbName");
					COUCHDB_PIPELINE_DB = config.getString("couchDbPipelineDbName");
					COUCHDB_MONITORING_DB = config.getString("couchDbMonitoringDbName");
					COUCHDB_CONNECTION_DB = config.getString("couchDbConnectionDbName");
					
					SESAME_REPOSITORY_ID = config.getString("sesameDbName");
					CONTEXT_PATH = config.getString("context_path");
					RDF_FORMAT = RDF_FORMAT.JSONLD;
					
					KAFKA_HOST = config.getString("kafkaHost");
					KAFKA_PROTOCOL = config.getString("kafkaProtocol");
					KAFKA_PORT = config.getInt("kafkaPort");
					
					JMS_HOST  = config.getString("jmsHost");;
					JMS_PROTOCOL = config.getString("jmsProtocol");
					JMS_PORT = config.getInt("jmsPort");
					
					ZOOKEEPER_HOST  = config.getString("zookeeperHost");
					ZOOKEEPER_PROTOCOL = config.getString("zookeeperProtocol");
					ZOOKEEPER_PORT = config.getInt("zookeeperPort");
					
					HIPPO_URL = config.getString("hippoUrl");
					PANDDA_URL = config.getString("panddaUrl");
					STREAMSTORY_URL = config.getString("streamStoryUrl");	
		
				} catch (Exception e) {
					e.printStackTrace();
					loadDefaults();
				} 
		}
		else {
			Logger.getAnonymousLogger().info("Loading defaults..");
			loadDefaults();
		}
	}
	
	private void loadDefaults() {
		// load defaults
					COUCHDB_USER_DB = "users";
					COUCHDB_PIPELINE_DB = "pipeline";
					COUCHDB_MONITORING_DB = "monitoring";
					COUCHDB_CONNECTION_DB = "connection"; 
					COUCHDB_PROTOCOL = "http";
					COUCHDB_HOSTNAME = "localhost";
					COUCHDB_PORT = 5984;
					
					SESAME_URI = "http://localhost:8080/openrdf-sesame";
					SESAME_REPOSITORY_ID = "test-6";
					
					HOSTNAME ="localhost";
					SERVER_URL = "http://" +HOSTNAME;
					
					ACTION_PORT = 8091;
					ESPER_PORT = 8090;
					ALGORITHM_PORT = 8092;
					SOURCES_PORT = 8089;
					
					ALGORITHM_BASE_URL = SERVER_URL +":" +ALGORITHM_PORT;
					ESPER_BASE_URL = SERVER_URL + ":" + ESPER_PORT;
					ACTION_BASE_URL = SERVER_URL + ":" + ACTION_PORT;
					SOURCES_BASE_URL = SERVER_URL + ":" + SOURCES_PORT;
					
					CONTEXT_PATH = "/semantic-epa-backend";
					
					KAFKA_HOST = "localhost";
					KAFKA_PROTOCOL = "http";
					KAFKA_PORT = 9092;
					
					JMS_HOST = "localhost";
					JMS_PROTOCOL = "tcp";
					JMS_PORT = 61616;
					
					TCP_SERVER_URL = "tcp://" +HOSTNAME;
					TCP_SERVER_PORT = 61616;
					
					WEBAPP_PORT = 8080;
					WEBAPP_BASE_URL = SERVER_URL + ":" + WEBAPP_PORT;
					
					ZOOKEEPER_HOST = "localhost";
					ZOOKEEPER_PORT = 2181;
					ZOOKEEPER_PROTOCOL = "http";
					
					HIPPO_URL = "";
					PANDDA_URL = "";
					STREAMSTORY_URL = "";	
					RDF_FORMAT = RDF_FORMAT.JSONLD;
	}

	public static Configuration getInstance() {
		if (null == instance) {
			instance = new Configuration();
			return instance;
		}
		return instance;
	}
	
	public static void update() {
		instance = new Configuration();
	}

	public PropertiesConfiguration getConfig() {
		return config;
	}


	// Default values if something can't be read from the config file
	


	public RDFWriter getRioWriter(OutputStream stream) throws RDFHandlerException
	{
		RDFWriter writer = Rio.createWriter(RDF_FORMAT, stream);

		writer.handleNamespace("sepa", "http://sepa.event-processing.org/sepa#");
		writer.handleNamespace("ssn", "http://purl.oclc.org/NET/ssnx/ssn#");
		writer.handleNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");
		writer.handleNamespace("empire", "urn:clarkparsia.com:empire:");
		writer.handleNamespace("fzi", "urn:fzi.de:sepa:");

		writer.getWriterConfig().set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);
		writer.getWriterConfig().set(JSONLDSettings.OPTIMIZE, true);

		return writer;
	}

	public static final BrokerConfig getBrokerConfig()
	{
		return BrokerConfig.CONFIGURED;
	}

	public final boolean isDemoMode()
	{
		return true;
	}

}
