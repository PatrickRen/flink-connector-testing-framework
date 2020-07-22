package org.apache.flink.connectors.e2e.common.util;

import java.io.File;

public abstract class FlinkJob {
	public String getJobName() {
		return this.getClass().getSimpleName();
	}
	public abstract File getJarFile() throws Exception;
	public abstract String getMainClassName();
	public abstract String[] getArguments();
}
