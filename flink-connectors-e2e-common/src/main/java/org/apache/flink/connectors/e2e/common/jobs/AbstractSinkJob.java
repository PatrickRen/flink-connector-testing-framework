package org.apache.flink.connectors.e2e.common.jobs;

import org.apache.flink.connectors.e2e.common.TestContext;
import org.apache.flink.connectors.e2e.common.source.ControllableSource;
import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public abstract class AbstractSinkJob extends FlinkJob {

	public void run(TestContext<String> testContext) throws Exception {
		File recordFile = new File(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "record.txt");
		ControllableSource controllableSource = new ControllableSource(recordFile.getAbsolutePath(), "END");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(controllableSource).addSink(testContext.sink());
		env.execute(testContext.jobName() + "-Sink");
	}

}
