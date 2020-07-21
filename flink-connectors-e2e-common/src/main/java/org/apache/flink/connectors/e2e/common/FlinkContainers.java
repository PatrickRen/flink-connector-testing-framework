package org.apache.flink.connectors.e2e.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Flink cluster running on containers.
 */
public class FlinkContainers extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkContainers.class);

	private final GenericContainer<?> jobManager;
	private final List<GenericContainer<?>> taskManagers;

	private final TemporaryFolder workspaceDirOutside = new TemporaryFolder();
	private static final File workspaceDirInside = new File("/workspace");
	private static final File jobDirInside = new File("/jobs");

	private RestClusterClient<String> client;

	/**
	 * Construct a flink container group
	 * @param jobManager Job manager container
	 * @param taskManagers List of task manager containers
	 */
	private FlinkContainers(GenericContainer<?> jobManager, List<GenericContainer<?>> taskManagers) {
		this.jobManager = Objects.requireNonNull(jobManager);
		this.taskManagers = Objects.requireNonNull(taskManagers);
	}

	/**
	 * Get a builder of org.apache.flink.connectors.e2e.common.FlinkContainers
	 * @param appName Name of the cluster
	 * @param numTaskManagers Number of task managers
	 * @return Builder of org.apache.flink.connectors.e2e.common.FlinkContainers
	 */
	public static Builder builder(String appName, int numTaskManagers) {
		return new Builder(appName, numTaskManagers);
	}

	@Override
	protected void before() throws Throwable {

		// Create temporary workspace and link it to Flink containers
		workspaceDirOutside.create();
		jobManager.withFileSystemBind(workspaceDirOutside.getRoot().getAbsolutePath(), workspaceDirInside.getAbsolutePath(), BindMode.READ_WRITE);
		taskManagers.forEach( tm -> tm.withFileSystemBind(workspaceDirOutside.getRoot().getAbsolutePath(), workspaceDirInside.getAbsolutePath(), BindMode.READ_WRITE));

		// Launch JM
		jobManager.start();
		LOG.info("Flink Job Manager is running on {}, with REST port {}", getJobManagerHost(), getJobManagerRESTPort());

		// Launch TMs
		taskManagers.forEach(GenericContainer::start);

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
		jobManager.stop();
		taskManagers.forEach(GenericContainer::stop);
		client.close();
	}

	// ---------------------------- Flink job controlling ---------------------------------

	public CompletableFuture<JobID> submitJob(FlinkJob job) {
		StreamExecutionEnvironment env = job.getJobEnvironment();
		return client.submitJob(env.getStreamGraph(job.getJobName()).getJobGraph());
	}

	public CompletableFuture<Void> cancelJob(JobID jobid) {
		return client.cancel(jobid).thenCompose(
				acknowledge -> waitJobCancelling(jobid)
		);
	}

	public CompletableFuture<Void> waitJobCancelling(JobID jobid) {
		CompletableFuture<Void> result = new CompletableFuture<>();
		CompletableFuture.runAsync(
			() -> {
				try {
					while (!client.getJobStatus(jobid).get().isTerminalState()) {}
					result.complete(null);
				} catch (Exception e) {
					result.completeExceptionally(e);
				}
			}
		);
		return result;
	}

	public Container.ExecResult copyAndSubmitJarJob(String jarPathOutside) throws Exception {
		// Validate JAR file first
		File jarFileOutside = new File(jarPathOutside);
		if (!jarFileOutside.exists()) {
			throw new FileNotFoundException("JAR file '" + jarPathOutside  + "' does not exist");
		}

		try {
			// Copy jar into job manager first
			jobManager.copyFileToContainer(MountableFile.forHostPath(jarPathOutside), jobDirInside.getAbsolutePath());
		} catch (Exception e) {
			LOG.error("Failed to copy JAR file into job manager container", e);
			throw new Exception(e);
		}
		Path jarPathInside = Paths.get(jobDirInside.getAbsolutePath(), jarFileOutside.getName());
		return submitJarJob(jarPathInside.toAbsolutePath().toString());
	}

	public Container.ExecResult submitJarJob(String jarPathInside) throws Exception {
		try {
			return jobManager.execInContainer("flink", "run", jarPathInside);
		} catch (Exception e) {
			LOG.error("Flink job submission failed", e);
			throw new Exception(e);
		}
	}

	// ---------------------------- Flink containers properties ------------------------------

	/**
	 * Get the hostname of job manager
	 * @return Hostname of job manager in string
	 */
	public String getJobManagerHost() {
		return jobManager.getHost();
	}

	/**
	 * Get the port of job master's REST service running on
	 * @return Port number in int
	 */
	public int getJobManagerRESTPort() {
		return jobManager.getMappedPort(Builder.JOBMANAGER_REST_PORT);
	}

	/**
	 * Get workspace folder
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

	//--------------------------- Introduce failure ---------------------------------
	/**
	 * Shutdown a task manager container and restart it
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
	 * Builder of org.apache.flink.connectors.e2e.common.FlinkContainers.
	 */
	public static final class Builder {

		private static final String FLINK_IMAGE_NAME = "flink:1.11.0-scala_2.11";
		private static final String JOBMANAGER_HOSTNAME = "jobmaster";
		private static final int JOBMANAGER_REST_PORT = 8081;

		private String appName;
		private final int numTaskManagers;
		private final Map<String, String> flinkProperties = new HashMap<>();
		private final Network flinkNetwork = Network.newNetwork();
		private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();

		public Builder(String appName, int numTaskManagers) {
			this.appName = appName;
			this.numTaskManagers = numTaskManagers;
			this.flinkProperties.put("jobmanager.rpc.address", JOBMANAGER_HOSTNAME);
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
						.withEnv("FLINK_PROPERTIES", toFlinkPropertiesString(flinkProperties))
						.withCommand("taskmanager")
						.withNetwork(flinkNetwork)
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
