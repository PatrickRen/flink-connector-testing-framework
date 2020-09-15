package org.apache.flink.connectors.e2e.common;

public enum SourceJobTerminationPattern {

	BOUNDED_SOURCE,

	DESERIALIZATION_SCHEMA,

	END_MARK,

	FORCE_STOP

}
