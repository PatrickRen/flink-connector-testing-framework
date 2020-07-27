package org.apache.flink.connectors.e2e.kafka.external;

import org.apache.flink.connectors.e2e.common.external.ExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystemFactory;

public class KafkaContainerizedExternalSystemFactory implements ExternalSystemFactory {

	@Override
	public ExternalSystem getExternalSystem() {
		return new KafkaContainerizedExternalSystem();
	}

	@Override
	public String name() {
		return KafkaContainerizedExternalSystem.NAME;
	}
}
