package org.apache.flink.connectors.e2e.common;

import org.junit.ClassRule;
import org.junit.Test;

public class FlinkContainerTest {
	@ClassRule
	public static FlinkContainers flink = FlinkContainers.builder("flink-container-unit-test", 1).build();

	@Test
	public void testCopyAndSubmitJob() {
		try {
			flink.copyAndSubmitJarJob("/Users/renqs/Workspaces/flink-jobs/KafkaSample/target/KafkaSample-1.0-SNAPSHOT.jar");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
