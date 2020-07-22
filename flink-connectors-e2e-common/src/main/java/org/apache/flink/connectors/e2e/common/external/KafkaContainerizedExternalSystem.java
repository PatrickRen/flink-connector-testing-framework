package org.apache.flink.connectors.e2e.common.external;

import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collections;
import java.util.Properties;

public class KafkaContainerizedExternalSystem extends ContainerizedExternalSystem {

	private Logger LOG = LoggerFactory.getLogger(KafkaContainerizedExternalSystem.class);
	public KafkaContainer kafka;
	public AdminClient kafkaAdminClient;

	public static String HOSTNAME = "kafka";

	public KafkaContainerizedExternalSystem(FlinkContainers flink) {
		LOG.info("Initializing KafkaContainerizedExternalSystem");
		kafka = new KafkaContainer()
				.dependsOn(flink.getJobManager())
				.withNetwork(flink.getJobManager().getNetwork())
				.withNetworkAliases(HOSTNAME);
	}

	public String getBootstrapServer() {
		return kafka.getBootstrapServers();
	}

	public void createTopic(String topicName, int numPartitions, short replicationFactor) throws Exception {
		// TODO: check kafka is running
		// TODO: return value
		NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
		try {
			kafkaAdminClient.createTopics(Collections.singletonList(newTopic)).all().get();
		} catch (Exception e) {
			LOG.error("Cannot create topic \"{}\"", topicName);
			throw new Exception(e);
		}
	}

	@Override
	protected void before() throws Throwable {
		kafka.start();
		Properties adminClientProps = new Properties();
		adminClientProps.put("bootstrap.servers", kafka.getBootstrapServers().split("//")[1]);
		kafkaAdminClient = AdminClient.create(adminClientProps);
	}

	@Override
	protected void after() {
		kafka.stop();
		kafkaAdminClient.close();
	}
}
