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

package org.apache.flink.connectors.e2e.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.e2e.common.SourceJobTerminationPattern;
import org.apache.flink.connectors.e2e.common.TestContext;
import org.apache.flink.connectors.e2e.common.jobs.FlinkJob;
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
	public SourceFunction<String> createSource() {
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
						return nextElement.equals(FlinkJob.END_MARK);
					}
				},
				kafkaProperties);
		kafkaSource.setStartFromEarliest();
		return kafkaSource;
	}

	@Override
	public SinkFunction<String> createSink() {
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
