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
import backtype.storm.contrib.jms.trident.JmsStateFactory;
import backtype.storm.contrib.jms.trident.JmsUpdater;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.hmsonline.storm.cassandra.trident.CassandraStateFactory;
import com.hmsonline.storm.cassandra.trident.CassandraUpdater;
import org.apache.storm.bolt.TridentWordCount;
import org.apache.storm.connector.ConnectorUtil;
import org.apache.storm.connector.TridentConnectorUtil;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.helper.SimpleCassandraTridentTupleMapper;
import org.apache.storm.spout.FileBasedBatchSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.TridentKafkaStateFactory;
import storm.kafka.trident.TridentKafkaUpdater;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.IBatchSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * {@inheritDoc}
 */
public class WordCountTridentSmokeTest extends AbstractWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTridentSmokeTest.class);

    private static final String TOPIC_NAME = "storm-smoke-test-trident";

    private static final String TABLE_NAME = "WordCounTrident";

    private static final String HDFS_SRC_DIR = "/tmp-trident/";

    private static final String HDFS_ROTATION_DIR = "/dest-trident/";

    private static final String COLUMN_FAMILY = "columnFamily";

    private static final String KEY_SPACE_NAME = "tridentSmokeTestKeyspace";

    private static final String JMS_QUEUE_NAME ="dynamicQueues/FOO.BAR";

    public WordCountTridentSmokeTest(String zkConnString, String kafkaBrokerlist, String hdfsUrl, String hbaseUrl,
                              String cassandraConnString, String jmsConnectionString) {
        super(zkConnString, kafkaBrokerlist, hdfsUrl, hbaseUrl, cassandraConnString, jmsConnectionString);
    }

    @Override
    public StormTopology buildTopology(Config topologyConf) throws Exception {
        IBatchSpout wordSpout = new FileBasedBatchSpout("words.txt", new Fields("word"), 10);

        TridentTopology topology = new TridentTopology();

        Stream wordsStream = topology.newStream("someWords", wordSpout);

        TridentKafkaStateFactory stateFactory = TridentConnectorUtil.getTridentKafkaStateFactory(TOPIC_NAME, kafkaBrokerlist, "word", "word", topologyConf);
        wordsStream.partitionPersist(stateFactory, new Fields("word"), new TridentKafkaUpdater(), new Fields()).parallelismHint(1);

        JmsStateFactory jmsStateFactory = TridentConnectorUtil.getJmsStateFactory(jmsConnectionString, JMS_QUEUE_NAME);
        wordsStream.partitionPersist(jmsStateFactory, new Fields("word"), new JmsUpdater(), new Fields()).parallelismHint(1);

        Stream kafkaStream = topology.newStream("kafkaTridentSpout",  TridentConnectorUtil.getTridentKafkaEmitter(zkConnString, TOPIC_NAME, topologyConf)).parallelismHint(1);
        Stream jmsStream = topology.newStream("jmsTridentSpout",  TridentConnectorUtil.getTridentJmsSpouts(jmsConnectionString, JMS_QUEUE_NAME, topologyConf, "words")).parallelismHint(1);

        kafkaStream = kafkaStream.global().each(new Fields("str"), new TridentWordCount(), new Fields("word","count")).parallelismHint(1);
        jmsStream = jmsStream.global().each(new Fields("words"), new TridentWordCount(), new Fields("word","count")).parallelismHint(1);

        HBaseStateFactory hBaseStateFactory = TridentConnectorUtil.getTridentHbaseFactory(hbaseUrl, TABLE_NAME, "word", COLUMN_FAMILY, Lists.newArrayList("word"),
                Lists.newArrayList("count"), topologyConf);
        TridentState tridentState = jmsStream.global().partitionPersist(hBaseStateFactory, new Fields("word", "count"), new HBaseUpdater(), new Fields()).parallelismHint(1);

        HdfsStateFactory tridentHdfsFactory = TridentConnectorUtil.getTridentHdfsFactory(hdfsUrl, HDFS_SRC_DIR, HDFS_ROTATION_DIR, "word", "count");
        kafkaStream.global().partitionPersist(tridentHdfsFactory, new Fields("word", "count"), new HdfsUpdater(), new Fields()).parallelismHint(1);

        CassandraStateFactory cassandraStateFactory = TridentConnectorUtil.getCassandraStateFactory(cassandraConnString, KEY_SPACE_NAME, "word", COLUMN_FAMILY, topologyConf);
        Map<String, Class> fieldToTypeMap = Maps.newHashMap();
        fieldToTypeMap.put("word", String.class);
        fieldToTypeMap.put("count", Long.class);
        SimpleCassandraTridentTupleMapper mapper = new SimpleCassandraTridentTupleMapper(KEY_SPACE_NAME, COLUMN_FAMILY, "word",fieldToTypeMap);
        kafkaStream.global().partitionPersist(cassandraStateFactory, new Fields("word", "count"),
                new CassandraUpdater(mapper), new Fields()).parallelismHint(1);
        return topology.build();
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

