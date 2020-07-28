package org.apache.flink.connectors.e2e.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.connectors.e2e.common.external.ContainerizedExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystemFactory;
import org.apache.flink.connectors.e2e.common.util.DatasetHelper;
import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.flink.connectors.e2e.common.util.FlinkJobInfo;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

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
		ServiceLoader<ExternalSystemFactory> externalSystemFactoryLoader = ServiceLoader.load(ExternalSystemFactory.class);
		Iterator<ExternalSystemFactory> factoryIterator = externalSystemFactoryLoader.iterator();
		List<ExternalSystemFactory> discoveredFactory = new ArrayList<>();

		while (factoryIterator.hasNext()) {
			discoveredFactory.add(factoryIterator.next());
		}

		// No external system factory was found
		if (discoveredFactory.isEmpty()) {
			throw new IllegalStateException("No external system factory was found");
		}

		//TODO: Currently only allow to provide one external system factory
		if (discoveredFactory.size() > 1) {
			throw new IllegalStateException("Multiple external system factories were found");
		}

		ExternalSystem externalSystem = discoveredFactory.get(0).getExternalSystem();
		if (externalSystem instanceof ContainerizedExternalSystem) {
			// Containerized external system should bind with Flink cluster first
			((ContainerizedExternalSystem)externalSystem).withFlinkContainers(flink);
		}
		return externalSystem;
	}


	/*------------------ Resources needed for the test -------------------*/
	protected File sourceFile;
	protected File destFile;
	public static final String INPUT_FILENAME = "random.txt";
	public static final String OUTPUT_FILENAME = "output.txt";
	public static final String END_MARK = "END";

	public void initResources() throws Exception {
		// Prepare random files
		sourceFile = new File(flink.getWorkspaceFolderOutside(), INPUT_FILENAME);
		DatasetHelper.writeRandomTextToFile(sourceFile, 100, 100);
		DatasetHelper.appendMarkToFile(sourceFile, END_MARK);
		destFile = Paths.get(flink.getWorkspaceFolderOutside().getAbsolutePath(), OUTPUT_FILENAME).toFile();
	}
	public void cleanupResources() {}



	/*------------------ Test result validation -------------------*/
	public boolean validateResult() throws Exception {
		DatasetHelper.appendMarkToFile(destFile, END_MARK);
		return DatasetHelper.isSame(sourceFile, destFile);
	}



	/*---------------------------- Test cases ----------------------------*/
	@Test
	public void testSourceSinkBasicFunctionality() throws Exception {

		LOG.info("Flink JM is running at {}:{}", flink.getJobManagerHost(), flink.getJobManagerRESTPort());
		LOG.info("Workspace path: {}", flink.getWorkspaceFolderOutside());

		// Preparation
		initResources();

		LOG.info("Submitting jobs to Flink containers...");
		// Submit two Flink jobs
		JobID sinkJobID = flink.submitJob(getSinkJob());
		LOG.info("Sink job submitted with JobID {}", sinkJobID);
		JobID sourceJobID = flink.submitJob(getSourceJob());
		LOG.info("Source job submitted with JobID {}", sourceJobID);

		// Wait for Flink job result
		LOG.info("Waiting for job...");
		// TODO: This should be wrapped using CompletableFuture
		JobStatus sinkJobStatus = flink.waitForJob(sinkJobID).get();
		LOG.info("Sink job status has transited to {}", sinkJobStatus);

		// Handling job failure
		if (sinkJobStatus == JobStatus.FAILED) {
			// Get job root exceptions
			JobExceptionsInfo exceptionsInfo = flink.getJobRootException(sinkJobID).get();
			LOG.error("Sink job failed with root exception: \n{}", exceptionsInfo.getRootException());
			throw new IllegalStateException("Sink job failed");
		}

		JobStatus sourceJobStatus = flink.waitForJob(sourceJobID).get();
		LOG.info("Source job status has transited to {}", sourceJobStatus);

		// Validate result
		Assert.assertTrue(validateResult());

		// Cleanup
		cleanupResources();
	}

	private FlinkJobInfo getSinkJob() throws Exception {
		return new FlinkJobInfo(FlinkJobInfo.JobType.SINK_JOB);
	}

	private FlinkJobInfo getSourceJob() throws Exception {
		return new FlinkJobInfo(FlinkJobInfo.JobType.SOURCE_JOB);
	}

}
