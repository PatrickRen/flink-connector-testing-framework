package org.apache.flink.connectors.e2e.common;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;

public interface TestContext<T> extends Serializable {

	String jobName();

	SourceFunction<T> createSource();

	SinkFunction<T> createSink();

	SourceJobTerminationPattern sourceJobTerminationPattern();
}
