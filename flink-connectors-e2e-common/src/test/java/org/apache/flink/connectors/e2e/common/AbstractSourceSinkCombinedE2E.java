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
import org.apache.flink.connectors.e2e.common.util.FlinkJobInfo;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.server.UnicastRef;
import sun.rmi.transport.tcp.TCPEndpoint;

import java.io.File;
import java.lang.reflect.Proxy;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RemoteObject;
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

	public void initResources() {}

	public void cleanupResources() {}

	/*------------------ Test result validation -------------------*/

	public boolean validateResult() throws Exception {
		File recordingFile = new File(flink.getWorkspaceFolderOutside(), "record.txt");
		File outputFile = new File(flink.getWorkspaceFolderOutside(), "output.txt");
		return DatasetHelper.isSame(recordingFile, outputFile);
	}

	/*---------------------------- Test cases ----------------------------*/
	@Test
	public void testSourceSinkWithControllableSource() throws Exception {

		LOG.info("Flink JM is running at {}:{}", flink.getJobManagerHost(), flink.getJobManagerRESTPort());
		LOG.info("Workspace path: {}", flink.getWorkspaceFolderOutside());
		LOG.info("ControllableSource is listening on port {}", flink.getTaskManagerRMIPort());

		// Preparation
		initResources();

		// Submit sink and source job
		LOG.info("Submitting jobs to Flink containers...");
		JobID sinkJobID = flink.submitJob(getSinkJob());
		LOG.info("Sink job submitted with JobID {}", sinkJobID);
		JobID sourceJobID = flink.submitJob(getSourceJob());
		LOG.info("Source job submitted with JobID {}", sourceJobID);

		// Wait for job ready
		LOG.info("Waiting for job ready...");
		flink.waitForJobStatus(sinkJobID, JobStatus.RUNNING).get();
		flink.waitForJobStatus(sourceJobID, JobStatus.RUNNING).get();

		// Get source controling stub
		SourceControlRpc stub = getSourceControlStub();

		// Emit 5 records
		stub.next();
		stub.next();
		stub.next();
		stub.next();
		stub.next();

		// Emit a lot of records
		stub.go();
		Thread.sleep(1000);

		// Stop emitting
		stub.pause();

		// Finish the job
		stub.finish();

		// Wait for job finish
		flink.waitForJobStatus(sinkJobID, JobStatus.FINISHED).get();
		flink.waitForJobStatus(sourceJobID, JobStatus.FINISHED).get();

		// Validate
		Assert.assertTrue(validateResult());

		cleanupResources();
	}

	/*--------------------- Flink job related ---------------------*/

	protected FlinkJobInfo getSinkJob() throws Exception {
		return new FlinkJobInfo(FlinkJobInfo.JobType.SINK_JOB);
	}

	protected FlinkJobInfo getSourceJob() throws Exception {
		return new FlinkJobInfo(FlinkJobInfo.JobType.SOURCE_JOB);
	}


	/*-------------------- ControllableSource stub ----------------------*/
	protected SourceControlRpc getSourceControlStub() throws Exception {
		LOG.info("Connecting to controllable source at {}:{}", ControllableSource.RMI_HOSTNAME, flink.getTaskManagerRMIPort());


		SourceControlRpc stub = (SourceControlRpc) LocateRegistry.getRegistry(
				ControllableSource.RMI_HOSTNAME,
				flink.getTaskManagerRMIPort()
		).lookup("SourceControl");

		// Hack into the dynamic proxy object to correct the port number
		TCPEndpoint ep = (TCPEndpoint)FieldUtils.readField(((UnicastRef)((RemoteObject)Proxy.getInvocationHandler(stub)).getRef()).getLiveRef(), "ep", true);
		FieldUtils.writeField(ep, "port", flink.getTaskManagerRMIPort(), true);

		return stub;
	}

}
