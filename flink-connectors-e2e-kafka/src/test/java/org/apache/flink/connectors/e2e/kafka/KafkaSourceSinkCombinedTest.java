package org.apache.flink.connectors.e2e.kafka;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connectors.e2e.common.AbstractSourceSinkCombinedTest;
import org.apache.flink.connectors.e2e.common.Dataset;
import org.apache.flink.connectors.e2e.common.FlinkContainers;
import org.apache.flink.connectors.e2e.common.external.ExternalSystem;
import org.apache.flink.connectors.e2e.common.external.KafkaContainerizedExternalSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.File;
import java.nio.file.Paths;
import java.util.Properties;

public class KafkaSourceSinkCombinedTest extends AbstractSourceSinkCombinedTest {

	private KafkaContainerizedExternalSystem kafka;
	File sourceFile;
	File destFile;

	@Override
	public ExternalSystem createExternalSystem() {
		kafka = new KafkaContainerizedExternalSystem(flink);
		return kafka;
	}

	@Override
	public SinkJob getSinkJob() {
		return new KafkaSinkJob();
	}

	@Override
	public SourceJob getSourceJob() {
		return new KafkaSourceJob();
	}

	@Override
	public void initResources() throws Exception {
		sourceFile = new File(flink.getWorkspaceFolderOutside(), "random.txt");
		Dataset.writeRandomTextToFile(sourceFile, 100, 100);
		destFile = Paths.get(flink.getWorkspaceFolderOutside().getAbsolutePath(), "bucket", "output-0-0.txt").toFile();
	}

	@Override
	public boolean validateResult() throws Exception {
		return Dataset.isSame(sourceFile, destFile);
	}

	class KafkaSinkJob extends SinkJob {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		public KafkaSinkJob() {
			env.enableCheckpointing(2000);
			env.setParallelism(1);

			File inputFile = new File(FlinkContainers.getWorkspaceDirInside(), "random.txt");
			String inputFilePath = inputFile.getAbsolutePath();
			TextInputFormat format = new TextInputFormat(new Path(inputFilePath));
			format.setFilesFilter(FilePathFilter.createDefaultFilter());
			format.setCharsetName("UTF-8");
			DataStream<String> stream = env.readFile(
					format,
					inputFilePath,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					10,
					BasicTypeInfo.STRING_TYPE_INFO
			);

			Properties kafkaProperties = new Properties();
			kafkaProperties.setProperty("bootstrap.servers", kafka.getBootstrapServer());
			kafkaProperties.setProperty("group.id", "source");
			stream.addSink(new FlinkKafkaProducer<String>(
					"source",
					new SimpleStringSchema(),
					kafkaProperties
			));
		}

		@Override
		public StreamExecutionEnvironment getJobEnvironment() {
			return env;
		}
	}

	class KafkaSourceJob extends SourceJob {

		public static final String OUTPUT_DIR = "output";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		public KafkaSourceJob() {
			env.enableCheckpointing(2000);
			env.setParallelism(1);
			Properties kafkaProperties = new Properties();
			kafkaProperties.setProperty("bootstrap.servers", kafka.getBootstrapServer());
			kafkaProperties.setProperty("group.id", "source");
			env.addSource(new FlinkKafkaConsumer<String>(
					"sink",
					new SimpleStringSchema(),
					kafkaProperties
			))
			.addSink(StreamingFileSink
					.forRowFormat(
							new Path(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), OUTPUT_DIR),
							new SimpleStringEncoder<String>("UTF-8")
					)
					.withBucketAssigner(new BasePathBucketAssigner<>())
					.withOutputFileConfig(new OutputFileConfig("output", ".txt"))
					.withRollingPolicy(OnCheckpointRollingPolicy.build())
					.build());
		}

		@Override
		public StreamExecutionEnvironment getJobEnvironment() {
			return env;
		}
	}
}
