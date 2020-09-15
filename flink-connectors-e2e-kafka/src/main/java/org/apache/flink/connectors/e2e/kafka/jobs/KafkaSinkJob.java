package org.apache.flink.connectors.e2e.kafka.jobs;

import org.apache.flink.connectors.e2e.common.jobs.AbstractSinkJob;
import org.apache.flink.connectors.e2e.kafka.KafkaTestContext;

public class KafkaSinkJob extends AbstractSinkJob {

	public static void main(String[] args) throws Exception {
		(new KafkaSinkJob()).run(new KafkaTestContext());
	}
}
