package org.apache.flink.connectors.e2e.kafka;

import org.apache.flink.connectors.e2e.common.AbstractSourceSinkCombinedE2E;
import org.apache.flink.connectors.e2e.common.TestContext;
import org.apache.flink.connectors.e2e.common.external.ExternalSystemFactory;
import org.apache.flink.connectors.e2e.kafka.external.KafkaContainerizedExternalSystemFactory;

public class KafkaSourceSinkCombinedE2E extends AbstractSourceSinkCombinedE2E {
	@Override
	protected ExternalSystemFactory getExternalSystemFactory() {
		return new KafkaContainerizedExternalSystemFactory();
	}

	@Override
	protected TestContext testContext() {
		return new KafkaTestContext();
	}
}
