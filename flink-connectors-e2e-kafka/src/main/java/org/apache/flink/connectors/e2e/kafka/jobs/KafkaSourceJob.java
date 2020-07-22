package org.apache.flink.connectors.e2e.kafka.jobs;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class KafkaSourceJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(60000);
		env.setParallelism(1);

		System.out.println(String.join(", ", args));

		final String destFilePath = args[0];
		final String kafkaServer = args[1];
		final String topicName = args[2];
		final String groupId = args[3];
		final String endMark = args[4];

		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", kafkaServer);
		kafkaProperties.setProperty("group.id", groupId);

		FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
				topicName,
				new SimpleStringSchema() {
					@Override
					public boolean isEndOfStream(String nextElement) {
						return nextElement.equals(endMark);
					}
				},
				kafkaProperties);
		kafkaSource.setStartFromEarliest();

		env.addSource(kafkaSource, "Kafka Source")
				.addSink(new SimpleFileSink(destFilePath)).name(destFilePath);
		env.execute("Kafka Source Job");
	}

	static class SimpleFileSink extends RichSinkFunction<String> {
		Logger LOG = LoggerFactory.getLogger(SimpleFileSink.class);
		String filePath;
		File sinkFile;
		BufferedWriter sinkBufferedWriter;

		SimpleFileSink(String filePath) throws Exception {
			this.filePath = filePath;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.sinkFile = new File(filePath);
			this.sinkBufferedWriter = new BufferedWriter(new FileWriter(sinkFile));
		}

		@Override
		public void close() throws Exception {
			LOG.info("Closing SimpleFlinkSink...");
			sinkBufferedWriter.flush();
			sinkBufferedWriter.close();
		}

		@Override
		public void invoke(String value, Context context) throws Exception {
			LOG.info("Invoked with value: {}", value);
			sinkBufferedWriter.append(value).append("\n");
		}
	}
}
