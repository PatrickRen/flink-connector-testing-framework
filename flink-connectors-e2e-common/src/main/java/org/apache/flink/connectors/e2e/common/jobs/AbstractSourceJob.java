/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.e2e.common.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.e2e.common.SourceJobTerminationPattern;
import org.apache.flink.connectors.e2e.common.TestContext;
import org.apache.flink.connectors.e2e.common.utils.FlinkContainers;
import org.apache.flink.connectors.e2e.common.utils.SuccessException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public abstract class AbstractSourceJob extends FlinkJob {

	public void run(TestContext<String> context) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (context.sourceJobTerminationPattern() == SourceJobTerminationPattern.END_MARK_FILTERING) {
			env.setRestartStrategy(RestartStrategies.noRestart());
		}

		File outputFile = new File(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "output.txt");

		DataStream<String> stream = env.addSource(context.createSource());

		switch (context.sourceJobTerminationPattern()) {
			case END_MARK_FILTERING:
				stream = stream.map((MapFunction<String, String>) value -> {
					if (value.equals(END_MARK)) {
						throw new SuccessException("Successfully received end mark");
					}
					return value;
				});
				break;
			case BOUNDED_SOURCE:
			case DESERIALIZATION_SCHEMA:
			case FORCE_STOP:
				break;
			default:
				throw new IllegalStateException("Unrecognized stop pattern");
		}
		stream.addSink(new SimpleFileSink(outputFile.getAbsolutePath(), true));
		env.execute(context.jobName() + "-Source");
	}

	static class SimpleFileSink extends RichSinkFunction<String> {
		Logger LOG = LoggerFactory.getLogger(SimpleFileSink.class);
		String filePath;
		File sinkFile;
		BufferedWriter sinkBufferedWriter;
		boolean flushPerRecord;

		SimpleFileSink(String filePath, boolean flushPerRecord) {
			this.filePath = filePath;
			this.flushPerRecord = flushPerRecord;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.sinkFile = new File(filePath);
			this.sinkBufferedWriter = new BufferedWriter(new FileWriter(sinkFile));
		}

		@Override
		public void close() throws Exception {
			LOG.info("Closing SimpleFlinkSink...");
			sinkBufferedWriter.flush();
			sinkBufferedWriter.close();
		}

		@Override
		public void invoke(String value, Context context) throws Exception {
			LOG.info("Invoked with value: {}", value);
			sinkBufferedWriter.append(value).append("\n");
			if (flushPerRecord) {
				sinkBufferedWriter.flush();
			}
		}
	}
}
