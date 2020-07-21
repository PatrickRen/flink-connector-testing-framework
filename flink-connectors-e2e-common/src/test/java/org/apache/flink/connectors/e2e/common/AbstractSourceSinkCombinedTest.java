package org.apache.flink.connectors.e2e.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.connectors.e2e.common.external.ExternalSystem;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@Ignore
public abstract class AbstractSourceSinkCombinedTest {

	public static Logger LOG = LoggerFactory.getLogger(AbstractSourceSinkCombinedTest.class);

	@ClassRule
	public static FlinkContainers flink = FlinkContainers
			.builder("source-sink-combined-test", 1)
			.build();

	@Rule
	public ExternalSystem externalSystem = createExternalSystem();

//	public abstract void setupExternalSystem();

	public abstract ExternalSystem createExternalSystem();
	public abstract SinkJob getSinkJob();
	public abstract SourceJob getSourceJob();
	public abstract void initResources() throws Exception;
	public abstract boolean validateResult() throws Exception;

	@Test
	public void testSourceSinkBasicFunctionality() throws Exception {

		LOG.info("Flink JM is running at {}:{}", flink.getJobManagerHost(), flink.getJobManagerRESTPort());
		LOG.info("Workspace path: {}", flink.getWorkspaceFolderOutside());

		// Preparation
		initResources();

		// Submit two Flink jobs
		CompletableFuture<JobID> submitSinkJobResult = flink.submitJob(getSinkJob());
		CompletableFuture<JobID> submitSourceJobResult = flink.submitJob(getSourceJob());

		JobID sinkJobId = submitSinkJobResult.get();
		System.out.println("Sink job " + sinkJobId + "submitted");
		JobID sourceJobId = submitSourceJobResult.get();
		System.out.println("Source job " + sourceJobId + "submitted");

		// Wait for Flink job result
		System.out.println("Waiting for job...");
		Thread.sleep(10);

		// Validate result
		Assert.assertTrue(validateResult());

		// Cleanup
		CompletableFuture<Void> cancelSinkJobResult = flink.cancelJob(sinkJobId);
		CompletableFuture<Void> cancelSourceJobResult = flink.cancelJob(sourceJobId);
		cancelSinkJobResult.get();
		cancelSourceJobResult.get();

	}

	public abstract static class SinkJob extends FlinkJob {}
	public abstract static class SourceJob extends FlinkJob {}
}
