package org.streampipes.model.grounding;

import org.streampipes.empire.annotations.RdfProperty;
import org.streampipes.empire.annotations.RdfsClass;
import org.streampipes.model.base.UnnamedStreamPipesEntity;
import org.streampipes.vocabulary.StreamPipes;

import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;

@RdfsClass(StreamPipes.TRANSPORT_PROTOCOL)
@Entity
@MappedSuperclass
public abstract class TransportProtocol extends UnnamedStreamPipesEntity {
	
	private static final long serialVersionUID = 7625791395504335184L;

	@RdfProperty(StreamPipes.BROKER_HOSTNAME)
	private String brokerHostname;
	
	@RdfProperty(StreamPipes.TOPIC)
	private String topicName;
	
	public TransportProtocol() {
		super();
	}
	
	public TransportProtocol(String uri, String topicName)
	{
		super();
		this.brokerHostname = uri;
		this.topicName = topicName;
	}

	public TransportProtocol(TransportProtocol other) {
		super(other);
		this.brokerHostname = other.getBrokerHostname();
		this.topicName = other.getTopicName();
	}

	public String getBrokerHostname() {
		return brokerHostname;
	}

	public void setBrokerHostname(String uri) {
		this.brokerHostname = uri;
	}
	
	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

}