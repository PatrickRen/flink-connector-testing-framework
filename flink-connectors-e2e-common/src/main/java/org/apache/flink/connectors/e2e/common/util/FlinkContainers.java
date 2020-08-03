package org.apache.flink.connectors.e2e.common.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connectors.e2e.common.source.ControllableSource;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Flink cluster running on containers.
 */
public class FlinkContainers extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkContainers.class);

	private final GenericContainer<?> jobManager;
	private final List<GenericContainer<?>> taskManagers;

	// Workspace directory for file exchange between testing framework and Flink containers
	private final TemporaryFolder workspaceDirOutside = new TemporaryFolder();
	private static final File workspaceDirInside = new File("/workspace");

	private final TemporaryFolder checkpointDirOutside = new TemporaryFolder();
	private static final File checkpointDirInside = new File("/checkpoint");

	// Job directory for saving job JARs inside Flink containers
	private static final File jobDirInside = new File("/jobs");

	// Flink client for monitoring job status
	private RestClusterClient<String> client;

	/**
	 * Construct a flink container group
	 *
	 * @param jobManager   Job manager container
	 * @param taskManagers List of task manager containers
	 */
	private FlinkContainers(GenericContainer<?> jobManager, List<GenericContainer<?>> taskManagers) {
		this.jobManager = Objects.requireNonNull(jobManager);
		this.taskManagers = Objects.requireNonNull(taskManagers);
	}

	/**
	 * Get a builder of org.apache.flink.connectors.e2e.common.util.FlinkContainers
	 *
	 * @param appName         Name of the cluster
	 * @param numTaskManagers Number of task managers
	 * @return Builder of org.apache.flink.connectors.e2e.common.util.FlinkContainers
	 */
	public static Builder builder(String appName, int numTaskManagers) {
		return new Builder(appName, numTaskManagers);
	}

	@Override
	protected void before() throws Throwable {

		// Create temporary workspace and link it to Flink containers
		workspaceDirOutside.create();
		jobManager.withFileSystemBind(workspaceDirOutside.getRoot().getAbsolutePath(), workspaceDirInside.getAbsolutePath(), BindMode.READ_WRITE);
		taskManagers.forEach(tm -> tm.withFileSystemBind(workspaceDirOutside.getRoot().getAbsolutePath(), workspaceDirInside.getAbsolutePath(), BindMode.READ_WRITE));

		// Create checkpoint folder and link to Flink containers
		checkpointDirOutside.create();
		jobManager.withFileSystemBind(checkpointDirOutside.getRoot().getAbsolutePath(), checkpointDirInside.getAbsolutePath(), BindMode.READ_WRITE);
		taskManagers.forEach(tm -> tm.withFileSystemBind(checkpointDirOutside.getRoot().getAbsolutePath(), checkpointDirInside.getAbsolutePath(), BindMode.READ_WRITE));

		// Launch JM
		jobManager.start();
		LOG.info("Flink Job Manager is running on {}, with REST port {}", getJobManagerHost(), getJobManagerRESTPort());

		// Launch TMs
		taskManagers.forEach(GenericContainer::start);
		LOG.info("{} Flink TaskManager(s) are running", taskManagers.size());

		// Flink configurations
		Configuration flinkConf = new Configuration();
		flinkConf.setString(JobManagerOptions.ADDRESS, getJobManagerHost());
		flinkConf.setInteger(RestOptions.PORT, getJobManagerRESTPort());

		// Prepare RestClusterClient
		Configuration clientConf = new Configuration();
		clientConf.setString(JobManagerOptions.ADDRESS, getJobManagerHost());
		clientConf.setInteger(RestOptions.PORT, getJobManagerRESTPort());
		// TODO: retries, delays
		client = new RestClusterClient<>(clientConf, "docker-cluster");
	}

	@Override
	protected void after() {
		workspaceDirOutside.delete();
		checkpointDirOutside.delete();
		jobManager.stop();
		LOG.info("Flink JobManager is stopped");
		taskManagers.forEach(GenericContainer::stop);
		LOG.info("{} Flink TaskManager(s) are stopped", taskManagers.size());
		client.close();
	}

	// ---------------------------- Flink job controlling ---------------------------------

	public JobID submitJob(File jarFileOutside, String mainClass) throws Exception {
		return copyAndSubmitJarJob(jarFileOutside, mainClass, null);
	}

	public JobID copyAndSubmitJarJob(File jarFileOutside, String mainClass, String[] args) throws Exception {
		// Validate JAR file first
		if (!jarFileOutside.exists()) {
			throw new FileNotFoundException("JAR file '" + jarFileOutside.getAbsolutePath() + "' does not exist");
		}

		try {
			// Copy jar into job manager first
			jobManager.copyFileToContainer(MountableFile.forHostPath(jarFileOutside.getAbsolutePath()), Paths.get(jobDirInside.getAbsolutePath(), jarFileOutside.getName()).toString());
		} catch (Exception e) {
			LOG.error("Failed to copy JAR file into job manager container", e);
			throw new Exception(e);
		}
		Path jarPathInside = Paths.get(jobDirInside.getAbsolutePath(), jarFileOutside.getName());
		return submitJarJob(jarPathInside.toAbsolutePath().toString(), mainClass, args);
	}

	public JobID submitJarJob(String jarPathInside, String mainClass, String[] args) throws Exception {
		LOG.info("Submitting job {} ...", mainClass);
		try {
			List<String> commandLine = new ArrayList<>();
			commandLine.add("flink");
			commandLine.add("run");
			commandLine.add("-d");
			commandLine.add("-c");
			commandLine.add(mainClass);
			commandLine.add(jarPathInside);
			if (args != null && args.length > 0) {
				commandLine.addAll(Arrays.asList(args));
			}
			LOG.debug("Executing command in JM: {}", String.join(" ", commandLine));
			Container.ExecResult result = jobManager.execInContainer(commandLine.toArray(new String[0]));
			if (result.getExitCode() != 0) {
				LOG.error("Command \"flink run\" exited with code {}. \nSTDOUT: {}\nSTDERR: {}",
						result.getExitCode(), result.getStdout(), result.getStderr());
				throw new IllegalStateException("Command \"flink run\" exited with code " + result.getExitCode());
			}
			LOG.debug(result.getStdout());
			JobID jobID = parseJobID(result.getStdout());
			LOG.info("Job {} has been submitted with JobID {}", mainClass, jobID);
			return jobID;

		} catch (Exception e) {
			LOG.error("Flink job submission failed", e);
			throw new Exception(e);
		}
	}

	public CompletableFuture<JobStatus> getJobStatus(JobID jobID) {
		return client.getJobStatus(jobID);
	}

	public CompletableFuture<Void> waitForJobStatus(JobID jobID, JobStatus expectedStatus) {
		return CompletableFuture.runAsync(
				() -> {
					JobStatus status = null;
					try {
						while (status == null || !status.equals(expectedStatus)) {
							status = getJobStatus(jobID).get();
							if (status.isTerminalState()) {
								break;
							}
						}
					} catch (Exception e) {
						LOG.error("Get job status failed", e);
						throw new CompletionException(e);
					}
					// If the job is entering an unexpected terminal status
					if (status.isTerminalState() && !status.equals(expectedStatus)) {
						try {
							LOG.error("Job has entered a terminal status {}, but expected {}", status, expectedStatus);
							if (status.equals(JobStatus.FAILED)) {
								JobExceptionsInfo exceptionsInfo = getJobRootException(jobID).get();
								LOG.error("Root exception of the job: \n{}", exceptionsInfo.getRootException());
							}
						} catch (Exception e) {
							LOG.error("Error when processing job status", e);
							throw new CompletionException(e);
						}
						throw new CompletionException(new IllegalStateException("Job has entered unexpected termination status"));
					}
				}
		);
	}

	public CompletableFuture<JobStatus> waitForJobTermination(JobID jobID) {
		return CompletableFuture.supplyAsync(
				() -> {
					JobStatus status = null;
					try {
						while (status == null || !status.isTerminalState()) {
							status = getJobStatus(jobID).get();
						}
					} catch (Exception e) {
						LOG.error("Get job status failed", e);
						throw new CompletionException(e);
					}
					return status;
				}
		);
	}

	public CompletableFuture<JobExceptionsInfo> getJobRootException(JobID jobID) {
		final JobExceptionsHeaders exceptionsHeaders = JobExceptionsHeaders.getInstance();
		final JobExceptionsMessageParameters params = exceptionsHeaders.getUnresolvedMessageParameters();
		params.jobPathParameter.resolve(jobID);
		return client.sendRequest(exceptionsHeaders, params, EmptyRequestBody.getInstance());
	}

	private JobID parseJobID(String stdoutString) throws Exception {
		Pattern pattern = Pattern.compile("JobID ([a-f0-9]*)");
		Matcher matcher = pattern.matcher(stdoutString);
		if (matcher.find()) {
			return JobID.fromHexString(matcher.group(1));
		} else {
			// TODO: Should specify a exception system and use a specific exception here
			throw new Exception("Cannot find JobID from the output of \"flink run\"");
		}
	}

	// ---------------------------- Flink containers properties ------------------------------

	/**
	 * Get the hostname of job manager
	 *
	 * @return Hostname of job manager in string
	 */
	public String getJobManagerHost() {
		return jobManager.getHost();
	}

	/**
	 * Get the port of job master's REST service running on
	 *
	 * @return Port number in int
	 */
	public int getJobManagerRESTPort() {
		return jobManager.getMappedPort(Builder.JOBMANAGER_REST_PORT);
	}

	/**
	 * Get workspace folder
	 *
	 * @return Workspace folder in File
	 */
	public File getWorkspaceFolderOutside() {
		return workspaceDirOutside.getRoot();
	}

	public static File getWorkspaceDirInside() {
		return workspaceDirInside;
	}

	public GenericContainer<?> getJobManager() {
		return jobManager;
	}

	public int getTaskManagerRMIPort() {
		// TODO: should support multiple TMs
		return taskManagers.get(0).getMappedPort(ControllableSource.RMI_PORT);
	}

	public List<Integer> getTaskManagerRMIPorts() {
		List<Integer> rmiPortList = new ArrayList<>();
		for (GenericContainer taskManager : taskManagers) {
			rmiPortList.add(taskManager.getMappedPort(ControllableSource.RMI_PORT));
		}
		return rmiPortList;
	}


	//--------------------------- Introduce failure ---------------------------------

	/**
	 * Shutdown a task manager container and restart it
	 *
	 * @param taskManagerIndex Index of task manager container to be restarted
	 */
	public void restartTaskManagers(int taskManagerIndex) {
		if (taskManagerIndex >= taskManagers.size()) {
			throw new IndexOutOfBoundsException("Invalid TaskManager index " + taskManagerIndex + ". Valid values are 0 to " + (taskManagers.size() - 1));
		}
		final GenericContainer<?> taskManager = taskManagers.get(taskManagerIndex);
		taskManager.stop();
		taskManager.start();
	}

	/**
	 * Builder of org.apache.flink.connectors.e2e.common.util.FlinkContainers.
	 */
	public static final class Builder {

		private static final String FLINK_IMAGE_NAME = "flink:1.11.0-scala_2.11";
		private static final String JOBMANAGER_HOSTNAME = "jobmaster";
		private static final int JOBMANAGER_REST_PORT = 8081;

		private final String appName;
		private final int numTaskManagers;
		private final Map<String, String> flinkProperties = new HashMap<>();
		private final Network flinkNetwork = Network.newNetwork();
		private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();

		public Builder(String appName, int numTaskManagers) {
			this.appName = appName;
			this.numTaskManagers = numTaskManagers;
			this.flinkProperties.put("jobmanager.rpc.address", JOBMANAGER_HOSTNAME);
			this.flinkProperties.put("state.checkpoints.dir", "file://" + checkpointDirInside.getAbsolutePath());
		}

		public FlinkContainers.Builder dependOn(GenericContainer<?> container) {
			container.withNetwork(flinkNetwork);
			dependentContainers.add(container);
			return this;
		}

		public FlinkContainers build() {
			return new FlinkContainers(
					createJobMasterContainer(flinkProperties, flinkNetwork, dependentContainers),
					createTaskManagerContainers(flinkProperties, flinkNetwork, numTaskManagers)
			);
		}

		private static GenericContainer<?> createJobMasterContainer(
				Map<String, String> flinkProperties,
				Network flinkNetwork,
				List<GenericContainer<?>> dependentContainers
		) {
			return new GenericContainer<>(FLINK_IMAGE_NAME)
					.withExposedPorts(JOBMANAGER_REST_PORT)
					.withEnv("FLINK_PROPERTIES", toFlinkPropertiesString(flinkProperties))
					.withCommand("jobmanager")
					.withNetwork(flinkNetwork)
					.withNetworkAliases(JOBMANAGER_HOSTNAME)
					.dependsOn(dependentContainers);
		}

		private static List<GenericContainer<?>> createTaskManagerContainers(
				Map<String, String> flinkProperties,
				Network flinkNetwork,
				int numTaskManagers
		) {
			List<GenericContainer<?>> taskManagers = new ArrayList<>();
			for (int i = 0; i < numTaskManagers; ++i) {
				taskManagers.add(
						new GenericContainer<>(FLINK_IMAGE_NAME)
								.withExposedPorts(ControllableSource.RMI_PORT)
								.withEnv("FLINK_PROPERTIES", toFlinkPropertiesString(flinkProperties))
								.withCommand("taskmanager")
								.withNetwork(flinkNetwork)
								.waitingFor(Wait.forLogMessage(".*Successful registration at resource manager.*", 1))
				);
			}
			return taskManagers;
		}

		private static String toFlinkPropertiesString(Map<String, String> flinkProperties) {
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<String, String> property : flinkProperties.entrySet()) {
				sb.append(property.getKey()).append(": ").append(property.getValue()).append("\n");
			}
			return sb.toString();
		}
	}
}
