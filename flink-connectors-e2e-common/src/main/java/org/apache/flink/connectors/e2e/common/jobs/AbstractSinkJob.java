package org.apache.flink.connectors.e2e.common.jobs;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.io.File;

public abstract class AbstractSinkJob extends FlinkJob {

	// TODO: should use generic type here instead of hard-code String
	public abstract SinkFunction<String> getSink();

	public void run(String jobName) throws Exception {
		File inputFile = new File(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "random.txt");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
		env.addSource(fileSource).transform(
				"Split Reader: " + inputFilePath,
				BasicTypeInfo.STRING_TYPE_INFO,
				new ContinuousFileReaderOperatorFactory<>(format)).addSink(getSink());

		env.execute(jobName);
	}
}
