package org.apache.flink.connectors.e2e.kafka.external;

import org.apache.flink.connectors.e2e.common.external.ContainerizedExternalSystem;
import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collections;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KafkaContainerizedExternalSystem extends ContainerizedExternalSystem<KafkaContainerizedExternalSystem> {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerizedExternalSystem.class);
	private final KafkaContainer kafka;
	private AdminClient kafkaAdminClient;

	public static final String NAME = "KafkaContainer";

	public static final String HOSTNAME = "kafka";

	public KafkaContainerizedExternalSystem() {
		kafka = new KafkaContainer()
				.withNetworkAliases(HOSTNAME);
	}

	public String getBootstrapServer() {
		return kafka.getBootstrapServers();
	}

	/**
	 * Create a topic in Kafka container. This is a blocking method which will wait for the result of
	 * topic creation.
	 * @param topicName Name of the new topic
	 * @param numPartitions Number of partitions
	 * @param replicationFactor Number of replications
	 * @throws Exception
	 */
	public void createTopic(String topicName, int numPartitions, short replicationFactor) throws Exception {
		// Make sure Kafka container is running
		Preconditions.checkState(kafka.isRunning(), "Kafka container is not running");

		NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
		try {
			kafkaAdminClient.createTopics(Collections.singletonList(newTopic)).all().get();
		} catch (Exception e) {
			LOG.error("Cannot create topic \"{}\"", topicName);
			throw new Exception(e);
		}
	}

	@Override
	public KafkaContainerizedExternalSystem withFlinkContainers(FlinkContainers flink) {
		this.flink = flink;
		kafka.dependsOn(flink.getJobManager())
				.withNetwork(flink.getJobManager().getNetwork());
		return this;
	}

	/*--------------------------- JUnit Lifecycle management ------------------------*/

	/**
	 * External system initialization. All preparation work should be done here because
	 * testing framework is aware of nothing about the external system
	 * @throws Throwable
	 */
	@Override
	protected void before() throws Throwable {
		// Make sure kafka container is bound with Flink containers
		checkNotNull(this.flink, "Kafka container is not bound with Flink containers." +
				"This will lead to network isolation between Kafka and Flink");

		// Start Kafka container
		kafka.start();

		// Start a Kafka admin client for management
		Properties adminClientProps = new Properties();
		adminClientProps.put("bootstrap.servers", kafka.getBootstrapServers().split("//")[1]);
		kafkaAdminClient = AdminClient.create(adminClientProps);

		LOG.info("Kafka container started.");

		// Create a topic
		createTopic("temp", 1, (short)1);
	}

	@Override
	protected void after() {
		kafkaAdminClient.close();
		kafka.stop();
	}
}
