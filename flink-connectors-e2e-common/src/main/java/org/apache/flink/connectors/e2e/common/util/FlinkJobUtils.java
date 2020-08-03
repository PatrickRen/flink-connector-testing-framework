package org.apache.flink.connectors.e2e.common.util;


import org.apache.flink.connectors.e2e.common.jobs.AbstractSinkJob;
import org.apache.flink.connectors.e2e.common.jobs.AbstractSourceJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class FlinkJobUtils {

	private static File searchedJarFile = null;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJobUtils.class);

	public static <J> String getJobClassName(Class<J> clazz) {
		ServiceLoader<J> sinkJobLoader = ServiceLoader.load(clazz);
		List<J> foundJobs = new ArrayList<>();
		for (J jobClass : sinkJobLoader) {
			foundJobs.add(jobClass);
		}
		if (foundJobs.isEmpty()) {
			throw new IllegalStateException("Cannot find any job class. Make sure you " +
					"1. Inherit AbstractSinkJob in your module; " +
					"2. Add your class in META-INF/services");
		}
		if (foundJobs.size() > 1) {
			throw new IllegalStateException("Multiple job classes were found. " +
					"Testing framework cannot decide which class to use if multiple entries are found. ");
		}
		return foundJobs.get(0).getClass().getName();
	}

	public static String getSinkJobClassName() throws Exception {
		return getJobClassName(AbstractSinkJob.class);
	}

	public static String getSourceJobClassName() throws Exception {
		return getJobClassName(AbstractSourceJob.class);
	}

	public static File searchJar() throws Exception {
		if (searchedJarFile == null) {
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
			searchedJarFile = jobJar;
		}
		LOG.info("Found JAR file {}", searchedJarFile.getName());
		return searchedJarFile;
	}
}
