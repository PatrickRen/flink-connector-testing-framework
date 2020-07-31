package org.apache.flink.connectors.e2e.kafka.jobs;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.e2e.common.jobs.AbstractSinkJob;
import org.apache.flink.connectors.e2e.kafka.external.KafkaContainerizedExternalSystem;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaSinkJob extends AbstractSinkJob {

	public static void main(String[] args) throws Exception {
		// (new KafkaSinkJob()).run("KafkaSinkJob");
		(new KafkaSinkJob()).run("Controllable Kafka sink job");
	}

	@Override
	public SinkFunction<String> getSink() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty(
				"bootstrap.servers",
				KafkaContainerizedExternalSystem.ENTRY
		);
		return new FlinkKafkaProducer<>(
				KafkaContainerizedExternalSystem.TOPIC,
				new SimpleStringSchema(),
				kafkaProperties
		);
	}
}
