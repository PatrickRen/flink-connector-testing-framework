/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.e2e.kafka.external;

import org.apache.flink.connectors.e2e.common.external.ContainerizedExternalSystem;
import org.apache.flink.connectors.e2e.common.utils.FlinkContainers;
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

	// Kafka container and its related AdminClient
	private final KafkaContainer kafka;
	private AdminClient kafkaAdminClient;

	// Name of this external system
	public static final String NAME = "KafkaContainer";

	// Hostname of Kafka in docker network, so that Flink can find it in network easily.
	public static final String HOSTNAME = "kafka";
	public static final int PORT = 9092;
	public static final String ENTRY = HOSTNAME + ":" + PORT;

	// Topic name for E2E test
	public static final String TOPIC = "flink-kafka-e2e-test";

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
	 *
	 * @param topicName         Name of the new topic
	 * @param numPartitions     Number of partitions
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

	/*------------------------------ Bind with Flink -------------------------------*/
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
	 *
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
		createTopic(TOPIC, 1, (short) 1);
	}

	@Override
	protected void after() {
		kafkaAdminClient.close();
		kafka.stop();
	}
}
