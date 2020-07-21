package org.apache.flink.connectors.e2e.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

public class FileSystemE2ETest {

	@ClassRule
	public static final FlinkContainers flink = FlinkContainers.builder("flink-test", 1)
			.build();


	@Test
	public void testFlinkContainer() throws Exception {
		Logger LOG = LoggerFactory.getLogger(FileSystemE2ETest.class);
		LOG.info("Flink JM is running on {}:{}", flink.getJobManagerHost(), flink.getJobManagerRESTPort());
		LOG.info("Workspace is {} on host", flink.getWorkspaceFolderOutside().getAbsolutePath());

		// Prepare random text file
		File randomTestFile = new File(flink.getWorkspaceFolderOutside(), "random.txt");
		Dataset.writeRandomTextToFile(randomTestFile, 100, 100);

		// Submit a job to Flink
		CompletableFuture<JobID> jobIdFuture =  flink.submitJob(new FileSourceSinkJob());
		JobID jobId = jobIdFuture.get();
		LOG.info("Job submitted with JobID {}", jobId);

		// Wait for the creation of output file
		File outputFile = Paths.get(flink.getWorkspaceFolderOutside().getAbsolutePath(), FileSourceSinkJob.OUTPUT_DIR, "output-0-0.txt").toFile();
		LOG.info("Waiting creation of output file");
		while(!outputFile.exists()) {}

		// Compare file
		LOG.info("Comparing two files");
		Assert.assertTrue(Dataset.isSame(randomTestFile, outputFile));

		// Cancel the job
		LOG.info("Cancelling the job");
		flink.cancelJob(jobId).whenComplete(
			(ignored, exception) -> {
				if (exception != null) {
					LOG.error("Error when canceling job {}", jobId, exception);
				} else {
					LOG.info("Job {} canceled", jobId);
				}
			}
		).get();
	}

	static class FileSourceSinkJob extends FlinkJob {

		public static final String OUTPUT_DIR = "output";

		private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		public FileSourceSinkJob() throws Exception {
			env.enableCheckpointing(5000);
			env.setParallelism(1);

			File inputFile = new File(FlinkContainers.getWorkspaceDirInside(), "random.txt");
			String inputFilePath = inputFile.getAbsolutePath();

			TextInputFormat format = new TextInputFormat(new Path(inputFilePath));
			format.setFilesFilter(FilePathFilter.createDefaultFilter());
			format.setCharsetName("UTF-8");

			DataStream<String> fileStream = env.readFile(
					format,
					inputFilePath,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					10,
					BasicTypeInfo.STRING_TYPE_INFO
			);
			StreamingFileSink<String> fileSink = StreamingFileSink
					.forRowFormat(
							new Path(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), OUTPUT_DIR),
							new SimpleStringEncoder<String>("UTF-8")
					)
					.withBucketAssigner(new BasePathBucketAssigner<>())
					.withOutputFileConfig(new OutputFileConfig("output", ".txt"))
					.withRollingPolicy(OnCheckpointRollingPolicy.build())
					.build();

			fileStream.addSink(fileSink);
		}

		@Override
		public StreamExecutionEnvironment getJobEnvironment() {
			return env;
		}
	}

}
