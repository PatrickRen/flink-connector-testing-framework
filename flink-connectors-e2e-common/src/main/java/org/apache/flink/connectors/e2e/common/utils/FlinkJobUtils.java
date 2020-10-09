/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.e2e.common.utils;

import org.apache.flink.connectors.e2e.common.jobs.AbstractSinkJob;
import org.apache.flink.connectors.e2e.common.jobs.AbstractSourceJob;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;
import java.util.Set;

/**
 * Utilities for searching Flink JAR files and main class of Flink jobs.
 */
public class FlinkJobUtils {

	private static File searchedJarFile = null;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJobUtils.class);

	/**
	 * Get name of main class of the job.
	 * @param pkg package to search in
	 * @param clazz abstract parent class of the searching job class
	 * @param <J> abstract parent class of the searching job class
	 * @return Name of main class of the job
	 */
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

	/**
	 * Get name of main class of the sink job.
	 * @param pkg package to search in
	 * @return Name of main class of the sink job
	 */
	public static String getSinkJobClassName(Package pkg) {
		return getJobClassName(pkg, AbstractSinkJob.class);
	}

	/**
	 * Get name of main class of the source job.
	 * @param pkg package to search in
	 * @return Name of main class of the source job
	 */
	public static String getSourceJobClassName(Package pkg) {
		return getJobClassName(pkg, AbstractSourceJob.class);
	}

	/**
	 * Search JAR file in target folder.
	 * @return JAR file
	 * @throws FileNotFoundException if JAR file is not found in target folder
	 */
	public static File searchJar() throws FileNotFoundException{
		if (searchedJarFile == null) {
			// Search JAR file in target directory
			String moduleName = new File(System.getProperty("user.dir")).getName();
			File targetDir = new File(System.getProperty("user.dir"), "target");
			File jobJar = null;
			for (File file : Objects.requireNonNull(targetDir.listFiles())) {
				String filename = file.getName();
				if (filename.startsWith(moduleName) && filename.endsWith(".jar")) {
					jobJar = file;
				}
			}
			if (jobJar == null) {
				throw new FileNotFoundException("Cannot find relative JAR file in the target directory. Make sure the maven project is built correctly.");
			}
			searchedJarFile = jobJar;
		}
		LOG.info("Found JAR file {}", searchedJarFile.getName());
		return searchedJarFile;
	}
}
