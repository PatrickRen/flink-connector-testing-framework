package org.apache.flink.connectors.e2e.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.e2e.common.SourceJobTerminationPattern;
import org.apache.flink.connectors.e2e.common.TestContext;
import org.apache.flink.connectors.e2e.kafka.external.KafkaContainerizedExternalSystem;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaTestContext implements TestContext<String> {
	@Override
	public String jobName() {
		return "KafkaConnectorTest";
	}

	@Override
	public SourceFunction<String> source() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty(
				"bootstrap.servers",
				KafkaContainerizedExternalSystem.ENTRY
		);
		kafkaProperties.setProperty("group.id", "test");

		FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
				KafkaContainerizedExternalSystem.TOPIC,
				new SimpleStringSchema() {
					@Override
					public boolean isEndOfStream(String nextElement) {
						return nextElement.equals("END");
					}
				},
				kafkaProperties);
		kafkaSource.setStartFromEarliest();
		return kafkaSource;
	}

	@Override
	public SinkFunction<String> sink() {
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

	@Override
	public SourceJobTerminationPattern sourceJobTerminationPattern() {
		return SourceJobTerminationPattern.DESERIALIZATION_SCHEMA;
	}
}
