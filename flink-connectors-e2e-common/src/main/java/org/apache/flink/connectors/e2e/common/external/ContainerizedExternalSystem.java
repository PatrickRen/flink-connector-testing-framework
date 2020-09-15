package org.apache.flink.connectors.e2e.common.external;

import org.apache.flink.connectors.e2e.common.utils.FlinkContainers;

/**
 * External system using <a href="https://www.testcontainers.org/">Testcontainers</a>
 */
public abstract class ContainerizedExternalSystem<T extends ContainerizedExternalSystem> extends ExternalSystem {
	protected FlinkContainers flink;

	public abstract T withFlinkContainers(FlinkContainers flink);
}
