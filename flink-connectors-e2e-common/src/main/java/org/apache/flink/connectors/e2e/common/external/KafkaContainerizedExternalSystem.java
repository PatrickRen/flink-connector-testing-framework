package org.apache.flink.connectors.e2e.common.external;

import org.apache.flink.connectors.e2e.common.FlinkContainers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

public class KafkaContainerizedExternalSystem extends ContainerizedExternalSystem {

	private Logger LOG = LoggerFactory.getLogger(KafkaContainerizedExternalSystem.class);
	public KafkaContainer kafka;

	public KafkaContainerizedExternalSystem(FlinkContainers flink) {
		LOG.info("Initializing KafkaContainerizedExternalSystem");
		kafka = new KafkaContainer()
				.withEmbeddedZookeeper()
				.dependsOn(flink.getJobManager())
				.withNetwork(flink.getJobManager().getNetwork());
	}

	public String getBootstrapServer() {
		return kafka.getBootstrapServers();
	}

	@Override
	protected void before() throws Throwable {
		kafka.start();
	}

	@Override
	protected void after() {
		kafka.stop();
	}
}
