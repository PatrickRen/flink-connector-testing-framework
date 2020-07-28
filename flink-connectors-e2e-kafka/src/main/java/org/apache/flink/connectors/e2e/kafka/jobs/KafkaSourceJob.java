package org.apache.flink.connectors.e2e.kafka.jobs;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.e2e.common.jobs.AbstractSourceJob;
import org.apache.flink.connectors.e2e.kafka.external.KafkaContainerizedExternalSystem;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceJob extends AbstractSourceJob {
	public static void main(String[] args) throws Exception {
		(new KafkaSourceJob()).run("KafkaSourceJob");
	}

	@Override
	public SourceFunction<String> getSource() {
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
}
