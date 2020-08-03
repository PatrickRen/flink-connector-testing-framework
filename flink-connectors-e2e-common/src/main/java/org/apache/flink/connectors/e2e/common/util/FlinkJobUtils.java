package org.apache.flink.connectors.e2e.common.util;


import org.apache.flink.connectors.e2e.common.jobs.AbstractSinkJob;
import org.apache.flink.connectors.e2e.common.jobs.AbstractSourceJob;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;

public class FlinkJobUtils {

	private static File searchedJarFile = null;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJobUtils.class);

	public static <J> String getJobClassName(Package pkg, Class<J> clazz) {

		LOG.info("Searching subclasses of {} in {}...", clazz.getSimpleName(), pkg.getName());

		Reflections reflections = new Reflections(pkg.getName());
		Set<Class<? extends J>> jobClasses = reflections.getSubTypesOf(clazz);

		if (jobClasses.isEmpty()) {
			throw new IllegalStateException("Cannot find any job class. Make sure you " +
					"have inherited " + clazz.getSimpleName() + " in your module.");
		}

		if (jobClasses.size() > 1) {
			throw new IllegalStateException("Multiple job classes were found. " +
					"Testing framework cannot decide which job to submit if multiple entries are found. ");
		}

		return jobClasses.iterator().next().getName();

	}

	public static String getSinkJobClassName(Package pkg) throws Exception {
		return getJobClassName(pkg, AbstractSinkJob.class);
	}

	public static String getSourceJobClassName(Package pkg) throws Exception {
		return getJobClassName(pkg, AbstractSourceJob.class);
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
