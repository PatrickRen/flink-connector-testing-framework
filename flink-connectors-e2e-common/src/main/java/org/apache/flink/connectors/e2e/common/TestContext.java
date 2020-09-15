package org.apache.flink.connectors.e2e.common;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface TestContext<T> {
	String jobName();
	SourceFunction<T> source();
	SinkFunction<T> sink();
	SourceJobTerminationPattern sourceJobTerminationPattern();
}
