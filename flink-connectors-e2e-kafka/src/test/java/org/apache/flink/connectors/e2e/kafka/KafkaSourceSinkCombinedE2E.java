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

package org.apache.flink.connectors.e2e.kafka;

import org.apache.flink.connectors.e2e.common.AbstractSourceSinkCombinedE2E;
import org.apache.flink.connectors.e2e.common.TestContext;
import org.apache.flink.connectors.e2e.common.external.ExternalSystemFactory;
import org.apache.flink.connectors.e2e.kafka.external.KafkaContainerizedExternalSystemFactory;

public class KafkaSourceSinkCombinedE2E extends AbstractSourceSinkCombinedE2E {
	@Override
	protected ExternalSystemFactory getExternalSystemFactory() {
		return new KafkaContainerizedExternalSystemFactory();
	}

	@Override
	protected TestContext testContext() {
		return new KafkaTestContext();
	}
}
