package org.apache.flink.connectors.e2e.common;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class FlinkJob {
	public abstract StreamExecutionEnvironment getJobEnvironment();
	public String getJobName() {
		return this.getClass().getSimpleName();
	}
}
