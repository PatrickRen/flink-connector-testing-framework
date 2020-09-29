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

import org.apache.flink.connectors.e2e.common.TestContext;
import org.apache.flink.connectors.e2e.common.source.ControllableSource;
import org.apache.flink.connectors.e2e.common.utils.FlinkContainers;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public abstract class AbstractSinkJob extends FlinkJob {

	public void run(TestContext<String> testContext) throws Exception {
		File recordFile = new File(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "record.txt");
		ControllableSource controllableSource = new ControllableSource(recordFile.getAbsolutePath(), END_MARK);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(controllableSource).addSink(testContext.createSink());
		env.execute(testContext.jobName() + "-Sink");
	}

}
