package org.apache.flink.connectors.e2e.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.e2e.common.external.ContainerizedExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystem;
import org.apache.flink.connectors.e2e.common.external.ExternalSystemFactory;
import org.apache.flink.connectors.e2e.common.util.FlinkContainers;
import org.apache.flink.connectors.e2e.common.util.FlinkJob;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

@Ignore
public abstract class AbstractSourceSinkCombinedE2E {

	public static Logger LOG = LoggerFactory.getLogger(AbstractSourceSinkCombinedE2E.class);

	@ClassRule
	public static FlinkContainers flink = FlinkContainers
			.builder("source-sink-combined-test", 1)
			.build();

	@Rule
	public ExternalSystem externalSystem = createExternalSystem();

	// External system related
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

	// Resources when running the test
	public abstract void initResources() throws Exception;
	public abstract void cleanupResources();

	// Result validation
	public abstract boolean validateResult() throws Exception;

	// Flink jobs used when running the test
	public abstract SinkJob getSinkJob();
	public abstract SourceJob getSourceJob();

	@Test
	public void testSourceSinkBasicFunctionality() throws Exception {

		LOG.info("Flink JM is running at {}:{}", flink.getJobManagerHost(), flink.getJobManagerRESTPort());
		LOG.info("Workspace path: {}", flink.getWorkspaceFolderOutside());

		// Preparation
		initResources();

		// Submit two Flink jobs
		JobID sinkJobID = flink.submitJob(getSinkJob());
		LOG.info("Sink job submitted with JobID {}", sinkJobID);
		JobID sourceJobID = flink.submitJob(getSourceJob());
		LOG.info("Source job submitted with JobID {}", sourceJobID);

		// Wait for Flink job result
		LOG.info("Waiting for job...");
		JobStatus sinkJobStatus = flink.waitForJob(sinkJobID).get();
		LOG.info("Sink job status has transited to {}", sinkJobStatus);
		JobStatus sourceJobStatus = flink.waitForJob(sourceJobID).get();
		LOG.info("Source job status has transited to {}", sourceJobStatus);

		// Validate result
		Assert.assertTrue(validateResult());

		// Cleanup
		cleanupResources();
	}

	public abstract static class SinkJob extends FlinkJob {}
	public abstract static class SourceJob extends FlinkJob {}
}
