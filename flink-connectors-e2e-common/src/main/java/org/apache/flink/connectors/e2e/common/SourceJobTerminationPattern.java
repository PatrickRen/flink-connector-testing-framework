package org.apache.flink.connectors.e2e.common;

public enum SourceJobTerminationPattern {

	/* Using new source API introduced in FLIP-27 and the source itself is bounded. */
	BOUNDED_SOURCE,

	/**
	 * Using {@link org.apache.flink.api.common.serialization.DeserializationSchema#isEndOfStream(Object)}
	 * to stop the source.
	 */
	DESERIALIZATION_SCHEMA,

	/**
	 * Using a record provided by testing framework to mark the end of stream. If this pattern is chosen, a map
	 * operator will be added between source and sink as a filter, and
	 * {@link org.apache.flink.connectors.e2e.common.utils.SuccessException} will be thrown, which will lead to failure
	 * of the job.
	 */
	END_MARK_FILTERING,

	/**
	 * The framework has to forcibly kill the job when some conditions are fulfilled.
	 */
	FORCE_STOP

}
