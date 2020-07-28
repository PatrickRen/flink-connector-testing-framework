package org.apache.flink.connectors.e2e.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FlinkJobInfo {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInfo.class);

	private static final String PROPERTIES_SINKJOB_MAIN_CLASS = "sinkjob.main.class";
	private static final String PROPERTIES_SOURCEJOB_MAIN_CLASS = "sourcejob.main.class";

	private final JobType jobType;
	private File jarFile;
	private final String mainClassName;
	private String[] args;

	public FlinkJobInfo(JobType type) throws Exception {
		this.jobType = type;
		// Get entry class name from properties
		Properties prop = new Properties();
		try {
			prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("e2e.properties"));
		} catch (Exception e) {
			LOG.error("Cannot load e2e.properties", e);
			throw new Exception(e);
		}
		switch (type) {
			case SINK_JOB:
				this.mainClassName = prop.getProperty(PROPERTIES_SINKJOB_MAIN_CLASS);
				break;
			case SOURCE_JOB:
				this.mainClassName = prop.getProperty(PROPERTIES_SOURCEJOB_MAIN_CLASS);
				break;
			default:
				throw new IllegalArgumentException("Unknown job type " + type);
		}
		if (mainClassName == null) {
			throw new IllegalStateException("Cannot find main class from e2e.properties file");
		}
		LOG.info("Found main name class {}", this.mainClassName);
	}

	public String getMainClassName() {
		return this.mainClassName;
	}

	public JobType getJobType() {
		return this.jobType;
	}

	public File getJarFile() throws Exception {
		if (this.jarFile == null) {
			// Search JAR file in target directory
			String moduleName = new File(System.getProperty("user.dir")).getName();
			File targetDir = new File(System.getProperty("user.dir"), "target");
			File jobJar = null;
			for (File file : targetDir.listFiles()) {
				String filename = file.getName();
				if (filename.startsWith(moduleName) && filename.endsWith(".jar")) {
					jobJar = file;
				}
			}
			if (jobJar == null) {
				throw new Exception("Cannot find relative JAR file in the target directory. Make sure the maven project is built correctly.");
			}
			this.jarFile = jobJar;
		}
		LOG.info("Found JAR file {}", this.jarFile.getName());
		return this.jarFile;
	}

	public String[] getArguments() {
		return null;
	}

	public enum JobType {
		SOURCE_JOB,
		SINK_JOB
	}
}
