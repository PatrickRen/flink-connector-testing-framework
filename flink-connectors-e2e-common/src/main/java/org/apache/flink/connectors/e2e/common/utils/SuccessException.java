package org.apache.flink.connectors.e2e.common.utils;

public class SuccessException extends RuntimeException {
	public SuccessException(String message) {
		super(message);
	}
}
