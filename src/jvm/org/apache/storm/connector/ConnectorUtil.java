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

package org.apache.storm.connector;

import backtype.storm.Config;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.CassandraBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.helper.SimpleJMSTuplePropducer;
import org.apache.storm.helper.SimpleJmsProducer;
import org.apache.storm.helper.SimpleJmsProvider;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.trident.GlobalPartitionInformation;

import javax.jms.Session;
import java.util.*;

/**
 * Provides various spout and bolt connectors.
 */
public class ConnectorUtil {

    public static KafkaBolt<String, String> getKafkaBolt(String topicName, String brokerList, String keyField,
                                                         String messageField, Map topologyConfig) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        topologyConfig.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        return new KafkaBolt<String, String>()
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>(keyField, messageField));
    }

    public static KafkaSpout getKafkaSpout(String zkConnString, String topicName) {
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        //spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }

    public static JmsBolt getJmsBolt(String jmsUrl, String queueName) throws Exception {
        JmsBolt jmsBolt = new JmsBolt();
        jmsBolt.setAutoAck(true);
        jmsBolt.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        jmsBolt.setJmsMessageProducer(new SimpleJmsProducer());
        jmsBolt.setJmsProvider(new SimpleJmsProvider(jmsUrl, queueName));

        return jmsBolt;
    }

    public static JmsSpout getJmsSpout(String jmsUrl, String queueName, String... fields) throws Exception {
        JmsSpout spout = new JmsSpout();
        spout.setJmsProvider(new SimpleJmsProvider(jmsUrl, queueName));
        spout.setJmsTupleProducer(new SimpleJMSTuplePropducer(new Fields(fields)));
        spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        spout.setRecoveryPeriod(10);
        return spout;
    }

    public static HdfsBolt getHdfsBolt(String fsUrl, String srcDir, String rotationDir) {
        // sync the filesystem after every tuple
        SyncPolicy syncPolicy = new CountSyncPolicy(1);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(srcDir)
                .withExtension(".txt");

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1f, FileSizeRotationPolicy.Units.KB);

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(fsUrl)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withSyncPolicy(syncPolicy)
                .withRotationPolicy(rotationPolicy)
                .addRotationAction(new MoveFileAction().toDestination(rotationDir));

        return bolt;
    }

    public static HBaseBolt getHBaseBolt(String hBaseUrl, String tableName, String rowKeyField, String columnFamily,
                                         List<String> columnFields, List<String> counterFields, Map topologyConfig) {
        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir",hBaseUrl);

        topologyConfig.put("hbase.conf", hbConf);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField(rowKeyField)
                .withColumnFields(new Fields(columnFields))
                .withCounterFields(new Fields(counterFields))
                .withColumnFamily(columnFamily);

        return new HBaseBolt(tableName, mapper)
                .withConfigKey("hbase.conf");
    }

    public static IRichBolt getCassandraBolt(String cassandraConnectionString, String keySpace, String columnFamily,
                                                 String rowKey, Map topologyConfig) {
        Map<String, Object> cassandraConfig = new HashMap<String, Object>();
        cassandraConfig.put(StormCassandraConstants.CASSANDRA_HOST, cassandraConnectionString);
        cassandraConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Lists.newArrayList(keySpace));
        String configKey = "cassandra-config";
        topologyConfig.put(configKey, cassandraConfig);

        TupleMapper<String, String, String> tupleMapper = new DefaultTupleMapper(keySpace, columnFamily, rowKey);

        topologyConfig.put(configKey, cassandraConfig);
        return new CassandraBatchingBolt<String, String, String>(configKey, tupleMapper);
    }

}
