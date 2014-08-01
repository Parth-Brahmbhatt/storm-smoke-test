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

package org.apache.storm.main;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.Validate;
import org.apache.storm.smoketest.SmokeTest;
import org.apache.storm.smoketest.WordCountSmokeTest;
import org.apache.storm.smoketest.WordCountTridentSmokeTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * This is the main class that runs all the tests and reports final success or failure.
 * You can add new smokeTests to the list of smokeTests.
 */
public class TestRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);

    private static final String USAGE_MSG = "Usage: TestRunner local/distributed path_to_properties_file.";
    /**
     *
     * @param args first arg can be local/distributed which represents the mode. second argument is location of properties file.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String mode = null;
        String propertyFile = null;
        if(args.length != 2 ) {
            System.err.println(USAGE_MSG);
            System.exit(-1);
        }

        mode = args[0];
        propertyFile = args[1];

        Properties prop = new Properties();
        //prop.load(TestRunner.class.getClassLoader().getResourceAsStream("test-config.properties"));
        prop.load(new FileInputStream(new File(propertyFile)));

        String zkConnString = prop.getProperty("zkUrl");
        String kafkaBroker = prop.getProperty("kafkaBroker");
        String hdfsUrl = prop.getProperty("hdfsUrl");
        String hbaseUrl = prop.getProperty("hbaseUrl");

        Validate.notEmpty(zkConnString, "property file must specify property zkUrl");
        Validate.notEmpty(kafkaBroker, "property file must specify property kafkaBroker");
        Validate.notEmpty(hdfsUrl, "property file must specify property hdfsUrl");
        Validate.notEmpty(hbaseUrl, "property file must specify property hbaseUrl");

        //TODO: Would be nice to use spring or guice so we can just inject new smoke test implementations.
        List<SmokeTest> smokeTests = Lists.newArrayList();
        smokeTests.add(new WordCountSmokeTest(zkConnString, kafkaBroker, hdfsUrl, hbaseUrl));
        smokeTests.add(new WordCountTridentSmokeTest(zkConnString, kafkaBroker, hdfsUrl, hbaseUrl));

        boolean success = true;
        Map<String, Throwable> testToErrorMap = Maps.newHashMap();
        for (SmokeTest smokeTest : smokeTests) {
            try {
                smokeTest.cleanup();
                smokeTest.setup();
                Config config = new Config();
                StormTopology stormTopology = smokeTest.buildTopology(config);
                String topologyName = smokeTest.getClass().getSimpleName() + RandomStringUtils.randomAlphanumeric(5);
                System.out.println("submitting topology: " + topologyName);

                if (mode.equalsIgnoreCase("local")) {
                    LocalCluster cluster = new LocalCluster();
                    cluster.submitTopology(topologyName, config, stormTopology);
                    Thread.sleep(smokeTest.getAllowedExecutionMills());
                    cluster.killTopology(topologyName);
                    cluster.shutdown();
                } else {
                    StormSubmitter.submitTopology(topologyName, config, stormTopology);
                    Thread.sleep(smokeTest.getAllowedExecutionMills());
                    //TODO: we need to kill the topologies in distributed mode.
                }
                smokeTest.verify();
            } catch (Throwable t) {
                LOG.error("Exception during test " + smokeTest.getClass().getName(), t);
                success = false;
                testToErrorMap.put(smokeTest.getClass().getName(), t);
            } finally {
                try {
                    smokeTest.cleanup();
                } catch (Throwable t) {
                    LOG.error("error during clenaup ", t);
                }
            }
        }

        reportResult(success, testToErrorMap);
    }


    public static void reportResult(boolean success, Map<String, Throwable> testToErrorMap) {
        if(success) {
            String msg = "all tests succeeded";
            System.out.println(msg);
            LOG.info(msg);
        } else {
            for(Map.Entry<String, Throwable> entry : testToErrorMap.entrySet()) {
                String msg = "Test " + entry.getKey() + " threw " + entry.getValue();
                System.err.println(msg);
                entry.getValue().printStackTrace();
                LOG.error(msg, entry.getValue());
            }
        }
    }
}
