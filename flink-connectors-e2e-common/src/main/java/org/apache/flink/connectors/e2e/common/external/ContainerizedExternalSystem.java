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

package org.apache.flink.connectors.e2e.common.external;

import org.apache.flink.connectors.e2e.common.utils.FlinkContainers;

/**
 * External system using <a href="https://www.testcontainers.org/">Testcontainers</a>.
 */
public abstract class ContainerizedExternalSystem<T extends ContainerizedExternalSystem<?>> extends ExternalSystem {
	protected FlinkContainers flink;

	/**
	 * Bind this containerized external system with Flink container.
	 *
	 * <p>The containerized external system has to be binded with flink containers so that they can access each other
	 * through network. </p>
	 * @param flink Flink containers
	 * @return The containerized external system itself
	 */
	public abstract T withFlinkContainers(FlinkContainers flink);
}
