package org.apache.flink.connectors.e2e.common.util;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.connectors.e2e.common.source.ControllableSource;
import org.apache.flink.connectors.e2e.common.source.SourceControlRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.List;

public class SourceController implements SourceControlRpc {

	private static final Logger LOG = LoggerFactory.getLogger(SourceController.class);

	private SourceControlRpc stub;

	public SourceController(List<Integer> potentialPorts) throws Exception {
		this(ControllableSource.RMI_HOSTNAME, potentialPorts);
	}

	public SourceController(String host, List<Integer> potentialPorts) throws Exception {
		int actualRMIPort = -1;

		for (Integer port : potentialPorts) {
			try {
				stub = (SourceControlRpc) LocateRegistry.getRegistry(
						ControllableSource.RMI_HOSTNAME,
						port
				).lookup("SourceControl");
				actualRMIPort = port;
				break;
			} catch (NotBoundException e) {
				// This isn't the task manager we want. Just skip it
			}
		}

		if (stub == null || actualRMIPort == -1) {
			throw new IllegalStateException("Cannot find any controllable source among task managers");
		}

		LOG.info("Connected to controllable source at {}:{}", ControllableSource.RMI_HOSTNAME, actualRMIPort);

		// Because of the mechanism of Java RMI, host and port registered in RMI registry would be LOCAL inside docker,
		// which is not accessible on docker host / testing framework.
		// So we "hack" into the dynamic proxy object created by Java RMI to correct the port number using reflection.
		FieldUtils.writeField(
				FieldUtils.readField(
						FieldUtils.readField(
								FieldUtils.readField(
										FieldUtils.readField(stub, "h", true),
										"ref", true),
								"ref", true),
						"ep", true),
				"port", actualRMIPort, true);
	}

	private SourceController() {
	}

	@Override
	public void pause() throws RemoteException {
		LOG.info("Pause");
		stub.pause();
	}

	@Override
	public void next() throws RemoteException {
		LOG.info("Next");
		stub.next();
	}

	@Override
	public void go() throws RemoteException {
		LOG.info("Go");
		stub.go();
	}

	@Override
	public void finish() throws RemoteException {
		LOG.info("Finish");
		stub.finish();
	}
}
