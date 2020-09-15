package org.apache.flink.connectors.e2e.kafka.jobs;

import org.apache.flink.connectors.e2e.common.jobs.AbstractSourceJob;
import org.apache.flink.connectors.e2e.kafka.KafkaTestContext;

public class KafkaSourceJob extends AbstractSourceJob {
	public static void main(String[] args) throws Exception {
		(new KafkaSourceJob()).run(new KafkaTestContext());
	}
}
