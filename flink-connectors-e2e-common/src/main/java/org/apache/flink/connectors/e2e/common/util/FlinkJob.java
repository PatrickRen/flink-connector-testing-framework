package org.apache.flink.connectors.e2e.common.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public abstract class FlinkJob {
	public String getJobName() {
		return this.getClass().getSimpleName();
	}
	public abstract File getJarFile();
	public abstract String getMainClassName();
	public abstract String[] getArguments();
}
