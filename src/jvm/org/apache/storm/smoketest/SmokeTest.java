/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.smoketest;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

import java.io.Serializable;

/**
 * The actual test interface that users should implement in order to create their own tests.
 */
public interface SmokeTest extends Serializable {
    /**
     * equivalent of junit's @Before.
     * @throws Exception
     */
    void setup() throws Exception;

    /**
     * equivalent of junit's @After. we also execute this as part of setup in case
     * previous test run left some garbage behind.
     * @throws Exception
     */
    void cleanup() throws Exception;

    /**
     * Construct the topology that you want to run. all modifications to topologyConf will be submitted along
     * with topology so feel free to mutate it.
     * @param topologyConf
     * @return
     * @throws Exception
     */
    StormTopology buildTopology(Config topologyConf) throws Exception;

    /**
     * How much time should the test runner wait for your topologies completion.
     * eqivalent to junit's TimeOut.
     * @return
     * @throws Exception
     */
    long getAllowedExecutionMills() throws Exception;

    /**
     * Perform all your assertions here.
     * @throws Exception
     */
    void verify() throws Exception;
}
