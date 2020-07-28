package org.apache.flink.connectors.e2e.common.jobs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public abstract class AbstractSourceJob extends FlinkJob {

	// TODO: should use generic type here instead of hard-code String
	public abstract SourceFunction<String> getSource();

	public void run(String jobName) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		File outputFile = new File(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "output.txt");
		env.addSource(getSource()).addSink(new SimpleFileSink(outputFile.getAbsolutePath()));
		env.execute(jobName);
	}

	static class SimpleFileSink extends RichSinkFunction<String> {
		Logger LOG = LoggerFactory.getLogger(SimpleFileSink.class);
		String filePath;
		File sinkFile;
		BufferedWriter sinkBufferedWriter;

		SimpleFileSink(String filePath) {
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
