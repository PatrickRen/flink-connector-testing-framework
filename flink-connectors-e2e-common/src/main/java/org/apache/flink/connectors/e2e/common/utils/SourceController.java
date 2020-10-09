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

import org.apache.flink.connectors.e2e.common.source.ControllableSource;
import org.apache.flink.connectors.e2e.common.source.SourceControlRpc;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.time.Duration;
import java.util.List;

public class SourceController implements SourceControlRpc {

	private static final Logger LOG = LoggerFactory.getLogger(SourceController.class);

	private SourceControlRpc stub;
	private List<Integer> potentialPorts;
	private boolean connected;

	public SourceController(List<Integer> potentialPorts) {
		this(ControllableSource.RMI_HOSTNAME, potentialPorts);
	}

	public SourceController(String host, List<Integer> potentialPorts) {
		this.potentialPorts = potentialPorts;
		connected = false;
	}

	private SourceController() {
	}

	public void connect() throws Exception {
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

		connected = true;

	}

	public void connect(Duration timeout) throws Exception {
		long deadline = System.currentTimeMillis() + timeout.toMillis();
		while (System.currentTimeMillis() < deadline) {
			try {
				connect();
			} catch (RemoteException e) {
				LOG.debug("Retrying connecting to remote object...");
				continue;
			}
			// Successfully connected to controllable source, jump out of the loop directly
			break;
		}
		if (!connected) {
			throw new IllegalStateException("Cannot connect to controllable source within " + timeout);
		}
	}

	@Override
	public void pause() throws RemoteException {
		ensureConnected();
		LOG.info("Pause");
		stub.pause();
	}

	@Override
	public void next() throws RemoteException {
		ensureConnected();
		LOG.info("Next");
		stub.next();
	}

	@Override
	public void go() throws RemoteException {
		ensureConnected();
		LOG.info("Go");
		stub.go();
	}

	@Override
	public void finish() throws RemoteException {
		ensureConnected();
		LOG.info("Finish");
		stub.finish();
	}

	private void ensureConnected() {
		if (!connected) {
			throw new IllegalStateException("Source controller is not connected to remote object");
		}
	}
}
