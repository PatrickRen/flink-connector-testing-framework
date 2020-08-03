package org.apache.flink.connectors.e2e.common;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.connectors.e2e.common.external.ContainerizedExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystemFactory;
import org.apache.flink.connectors.e2e.common.source.ControllableSource;
import org.apache.flink.connectors.e2e.common.source.SourceControlRpc;
import org.apache.flink.connectors.e2e.common.util.DatasetHelper;
import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.flink.connectors.e2e.common.util.FlinkJobUtils;
import org.apache.flink.connectors.e2e.common.util.SourceController;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;

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
	protected File sourceFile;
	protected File destFile;
	public static final String INPUT_FILENAME = "random.txt";
	public static final String OUTPUT_FILENAME = "output.txt";
	public static final String END_MARK = "END";

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
		return DatasetHelper.isSame(recordingFile, outputFile);
	}

	/*---------------------------- Test cases ----------------------------*/
	@Test
	public void testSourceSinkWithControllableSource() throws Exception {

		LOG.info("Flink JM is running at {}:{}", flink.getJobManagerHost(), flink.getJobManagerRESTPort());
		LOG.info("Workspace path: {}", flink.getWorkspaceFolderOutside());
		LOG.info("ControllableSource is listening on one of these ports: {}", flink.getTaskManagerRMIPorts());

		// Preparation
		initResources();

		// Submit sink and source job
		JobID sinkJobID = flink.submitJob(FlinkJobUtils.searchJar(), FlinkJobUtils.getSinkJobClassName());
		JobID sourceJobID = flink.submitJob(FlinkJobUtils.searchJar(), FlinkJobUtils.getSourceJobClassName());

		// Wait for job ready
		LOG.info("Wait until jobs are ready...");
		flink.waitForJobStatus(sinkJobID, JobStatus.RUNNING).get();
		flink.waitForJobStatus(sourceJobID, JobStatus.RUNNING).get();

		// Get source controling stub
		SourceController sourceController = new SourceController(flink.getTaskManagerRMIPorts());

		// Emit 5 records
		sourceController.next();
		sourceController.next();
		sourceController.next();
		sourceController.next();
		sourceController.next();

		// Emit a lot of records
		sourceController.go();
		Thread.sleep(1000);

		// Stop emitting
		sourceController.pause();

		// Finish the job
		sourceController.finish();

		// Wait for job finish
		flink.waitForJobStatus(sinkJobID, JobStatus.FINISHED).get();
		flink.waitForJobStatus(sourceJobID, JobStatus.FINISHED).get();

		// Validate
		Assert.assertTrue(validateResult());

		cleanupResources();

	}

}
