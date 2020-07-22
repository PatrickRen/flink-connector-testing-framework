package org.apache.flink.connectors.e2e.kafka.jobs;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.util.Properties;

public class KafkaSinkJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(60000);
		env.setParallelism(1);

		System.out.println(String.join(", ", args));

		String sourceFilePath = args[0];
		String kafkaServer = args[1];
		String topicName = args[2];
		String groupId = args[3];
		String endMark = args[4];

		File inputFile = new File(sourceFilePath);
		String inputFilePath = inputFile.getAbsolutePath();
		TextInputFormat format = new TextInputFormat(new Path(inputFilePath));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());
		format.setCharsetName("UTF-8");

		ContinuousFileMonitoringFunction<String> fileSource = new ContinuousFileMonitoringFunction<>(
				format,
				FileProcessingMode.PROCESS_ONCE,
				env.getParallelism(),
				10
		);

		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", kafkaServer);
		kafkaProperties.setProperty("group.id", groupId);

		FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(topicName,
				new SimpleStringSchema(),
				kafkaProperties);

		env.addSource(fileSource, sourceFilePath)
				.transform(
						"Split Reader: " + sourceFilePath,
						BasicTypeInfo.STRING_TYPE_INFO,
						new ContinuousFileReaderOperatorFactory<>(format))
				.addSink(kafkaSink).name("Kafka Sink");

		env.execute("Kafka Sink Job");
	}
}
