package org.apache.flink.connectors.e2e.common.source;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SourceControlRpc extends Remote {
	void pause() throws RemoteException;
	void next() throws RemoteException;
	void go() throws RemoteException;
	void finish() throws RemoteException;
}
