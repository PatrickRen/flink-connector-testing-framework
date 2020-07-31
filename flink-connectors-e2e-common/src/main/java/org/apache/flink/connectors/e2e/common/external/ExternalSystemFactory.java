package org.apache.flink.connectors.e2e.common.external;

public interface ExternalSystemFactory {
	ExternalSystem getExternalSystem();

	String name();
}
