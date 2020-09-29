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

package org.apache.flink.connectors.e2e.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.connectors.e2e.common.external.ContainerizedExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystemFactory;
import org.apache.flink.connectors.e2e.common.utils.DatasetHelper;
import org.apache.flink.connectors.e2e.common.utils.FlinkContainers;
import org.apache.flink.connectors.e2e.common.utils.FlinkJobUtils;
import org.apache.flink.connectors.e2e.common.utils.SourceController;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

@Ignore
public abstract class AbstractSourceSinkCombinedE2E {

	public static Logger LOG = LoggerFactory.getLogger(AbstractSourceSinkCombinedE2E.class);

	/*---------------------- JUnit lifecycle managed rules------------------------*/
	@ClassRule
	public static FlinkContainers flink = FlinkContainers
			.builder("source-sink-combined-test", 1)
			.build();

	@Rule
	public ExternalSystem externalSystem = createExternalSystem();

	public ExternalSystem createExternalSystem() {
		ExternalSystem externalSystem = getExternalSystemFactory().getExternalSystem();
		if (externalSystem instanceof ContainerizedExternalSystem) {
			((ContainerizedExternalSystem) externalSystem).withFlinkContainers(flink);
		}
		return externalSystem;
	}

	protected abstract ExternalSystemFactory getExternalSystemFactory();

	/*------------------ Resources needed for the test -------------------*/
	public void initResources() {
		LOG.info("Initializing test resources...");
	}

	public void cleanupResources() {
		LOG.info("Cleaning up test resources...");
	}

	/*------------------ Test result validation -------------------*/

	public boolean validateResult() throws Exception {
		LOG.info("Validating test result...");
		File recordingFile = new File(flink.getWorkspaceFolderOutside(), "record.txt");
		File outputFile = new File(flink.getWorkspaceFolderOutside(), "output.txt");
		boolean result = DatasetHelper.isSame(recordingFile, outputFile);
		if (!result) {
			flink.keepTestScene();
		}
		return result;
	}

	/*---------------------------- Test cases ----------------------------*/
	@Test
	public void testSourceSinkWithControllableSource() throws Exception {

		LOG.info("Flink JM is running at {}:{}", flink.getJobManagerHost(), flink.getJobManagerRESTPort());
		LOG.info("Workspace path: {}", flink.getWorkspaceFolderOutside());
		LOG.info("ControllableSource is listening on one of these ports: {}", flink.getTaskManagerRMIPorts());

		// STEP 0: Preparation
		initResources();

		// STEP 1: Sink job
		LOG.info("Submitting sink job and wait until job is ready...");
		JobID sinkJobID = flink.submitJob(FlinkJobUtils.searchJar(), FlinkJobUtils.getSinkJobClassName(getClass().getPackage()));
		flink.waitForJobStatus(sinkJobID, JobStatus.RUNNING).get();

		LOG.info("Start testing...");
		// Grab the source controller stub
		SourceController sourceController = new SourceController(flink.getTaskManagerRMIPorts());
		sourceController.connect(Duration.ofSeconds(10));

		LOG.info("Emitting 5 records...");
		sourceController.next();
		sourceController.next();
		sourceController.next();
		sourceController.next();
		sourceController.next();

		LOG.info("Emitting a lot of records...");
		sourceController.go();
		Thread.sleep(1000);
		// Stop emitting
		sourceController.pause();

		LOG.info("Stopping the sink job...");
		// Finish the source job
		sourceController.finish();
		flink.waitForJobStatus(sinkJobID, JobStatus.FINISHED).get();

		// STEP 2: Source job
		LOG.info("Submitting source job and wait until job is ready...");
		JobID sourceJobID = flink.submitJob(FlinkJobUtils.searchJar(), FlinkJobUtils.getSourceJobClassName(getClass().getPackage()));
		flink.waitForJobStatus(sourceJobID, JobStatus.RUNNING).get();

		LOG.info("Waiting for job finishing...");

		switch (testContext().sourceJobTerminationPattern()) {
			case END_MARK_FILTERING:
				flink.waitForFailingWithSuccessException(sourceJobID).get();
				break;
			case FORCE_STOP:
				// We need to wait until we get all records in the file, and then kill the job
			case DESERIALIZATION_SCHEMA:
			case BOUNDED_SOURCE:
				flink.waitForJobStatus(sourceJobID, JobStatus.FINISHED).get();
				break;
			default:
				throw new IllegalStateException("Unrecognized termination pattern");
		}

		// STEP 3: Validate
		Assert.assertTrue(validateResult());

		// STEP 4: Cleanup
		cleanupResources();
	}

	protected abstract TestContext testContext();
}
