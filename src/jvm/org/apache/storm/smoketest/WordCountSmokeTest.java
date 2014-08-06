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
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import org.apache.storm.bolt.WordCount;
import org.apache.storm.cleanup.CleanupUtils;
import org.apache.storm.connector.ConnectorUtil;
import org.apache.storm.setup.SetupUtils;
import org.apache.storm.spout.FileBasedSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 */
public class WordCountSmokeTest extends AbstractWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountSmokeTest.class);

    private static final String TOPIC_NAME = "storm-smoke-test";

    private static final String TABLE_NAME = "WordCount";

    private static final String HDFS_SRC_DIR = "/tmp/";

    private static final String HDFS_ROTATION_DIR = "/dest/";

    private static final String COLUMN_FAMILY = "columnFamily";

    private static final String KEY_SPACE_NAME = "smokeTestKeyspace";

    private static final String JMS_QUEUE_NAME ="dynamicQueues/FOO.BAR";

    public WordCountSmokeTest(String zkConnString, String kafkaBrokerlist, String hdfsUrl, String hbaseUrl,
                              String cassandraConnString, String jmsConnectionString) {
        super(zkConnString, kafkaBrokerlist, hdfsUrl, hbaseUrl, cassandraConnString, jmsConnectionString);
    }


    @Override
    public StormTopology buildTopology(Config topologyConf) throws Exception {
        IRichSpout wordSpout = new FileBasedSpout("words.txt", new Fields("word"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomWords", wordSpout, 1);

        builder.setBolt("writeToKafka", ConnectorUtil.getKafkaBolt(TOPIC_NAME, kafkaBrokerlist,
                "word", "word", topologyConf), 1).globalGrouping("randomWords");

        builder.setBolt("writeToJMS", ConnectorUtil.getJmsBolt(jmsConnectionString, JMS_QUEUE_NAME), 1)
                .globalGrouping("randomWords");

        builder.setSpout("kafkaSpout", ConnectorUtil.getKafkaSpout(zkConnString, TOPIC_NAME));

        builder.setSpout("jmsSpout", ConnectorUtil.getJmsSpout(jmsConnectionString, JMS_QUEUE_NAME, "word"));

        builder.setBolt("wordCountKafka", new WordCount(), 1).globalGrouping("kafkaSpout");

        builder.setBolt("wordCountJMS", new WordCount(), 1).globalGrouping("jmsSpout");

        builder.setBolt("hdfsBolt",
                ConnectorUtil.getHdfsBolt(hdfsUrl, HDFS_SRC_DIR, HDFS_ROTATION_DIR), 1).globalGrouping("wordCountKafka");

        builder.setBolt("hbaseBolt",
                ConnectorUtil.getHBaseBolt(hbaseUrl, TABLE_NAME, "word", COLUMN_FAMILY, Lists.newArrayList("word"),
                        Lists.newArrayList("count"), topologyConf), 1).globalGrouping("wordCountJMS");

        builder.setBolt("cassandraBolt",
                ConnectorUtil.getCassandraBolt(cassandraConnString, KEY_SPACE_NAME, COLUMN_FAMILY, "word", topologyConf), 1)
                .globalGrouping("wordCountKafka");

        return builder.createTopology();
    }

    @Override
    public String getTopicName() {
        return TOPIC_NAME;
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public String getColumnFamily() {
        return COLUMN_FAMILY;
    }

    @Override
    public String getHDfsSrcDir() {
        return HDFS_SRC_DIR;
    }

    @Override
    public String getHDfsDestDir() {
        return HDFS_ROTATION_DIR;
    }

    @Override
    public String getKeySpaceName() {
        return KEY_SPACE_NAME;
    }

    protected String getQueueName() {
        return JMS_QUEUE_NAME;
    }
}

