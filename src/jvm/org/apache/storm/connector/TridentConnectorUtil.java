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

import backtype.storm.contrib.jms.TridentJmsSpout;
import backtype.storm.contrib.jms.trident.JmsState;
import backtype.storm.contrib.jms.trident.JmsStateFactory;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.trident.CassandraStateFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.helper.SimpleJMSTuplePropducer;
import org.apache.storm.helper.SimpleJmsProvider;
import org.apache.storm.helper.SimpleTridentJmsMessageProducer;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentKafkaStateFactory;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.spout.ITridentSpout;

import javax.jms.Session;
import java.util.*;

/**
 * Provides various trident spout and bolt connectors.
 */
public class TridentConnectorUtil {
    public static OpaqueTridentKafkaSpout getTridentKafkaEmitter(String zkConnString, String topicName, Map topologyConfig) {
        BrokerHosts hosts = new ZkHosts(zkConnString);
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topicName);
        //topologyConfig.put("topology.spout.max.batch.size", 1);
        //kafkaConfig.forceFromStart = true;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new OpaqueTridentKafkaSpout(kafkaConfig);
    }

    public static TridentKafkaStateFactory getTridentKafkaStateFactory(String topicName, String brokerList, String keyField, String messageField, Map topologyConfig) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        topologyConfig.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        return new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector(topicName))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper(keyField, messageField));
    }

    public static JmsStateFactory getJmsStateFactory(String jmsUrl, String queueName) throws Exception {
        JmsState.Options options = new JmsState.Options()
                .withJmsProvider(new SimpleJmsProvider(jmsUrl, queueName))
                .withJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE)
                .withJmsTransactional(true)
                .withMessageProducer(new SimpleTridentJmsMessageProducer());
        return new JmsStateFactory(options);
    }

    public static TridentJmsSpout getTridentJmsSpouts(String jmsUrl, String queueName, Map topologyConfig, String... fields) throws Exception {
        TridentJmsSpout spout = new TridentJmsSpout()
                .named("jmsSpout")
                .withJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE)
                .withJmsProvider(new SimpleJmsProvider(jmsUrl, queueName))
                .withTupleProducer(new SimpleJMSTuplePropducer(new Fields(fields)));

        return spout;
    }

    public static HdfsStateFactory getTridentHdfsFactory(String fsUrl, String srcDir, String rotationDir, String... fields) {
        Fields hdfsFields = new Fields(fields);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(srcDir)
                .withExtension(".txt");

        RecordFormat recordFormat = new DelimitedRecordFormat().withFields(hdfsFields);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1f, FileSizeRotationPolicy.Units.KB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withFsUrl(fsUrl)
                .withRotationPolicy(rotationPolicy);

        return new HdfsStateFactory().withOptions(options);
    }

    public static HBaseStateFactory getTridentHbaseFactory(String hBaseUrl, String tableName, String rowKeyField, String columnFamily,
                                         List<String> columnFields, List<String> counterFields, Map topologyConfig) {
        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir",hBaseUrl);

        topologyConfig.put("hbase.conf", hbConf);

        TridentHBaseMapper tridentHBaseMapper = new SimpleTridentHBaseMapper()
                .withColumnFamily(columnFamily)
                .withColumnFields(new Fields(columnFields))
                .withCounterFields(new Fields(counterFields))
                .withRowKeyField(rowKeyField);

        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey("hbase.conf")
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withTableName(tableName);

        return new HBaseStateFactory(options);
    }

    public static CassandraStateFactory getCassandraStateFactory(String cassandraUrl, String keyspaceName,
                                                             String rowKeyField, String columnFamily,
                                                             Map topologyConfig) {
        Map<String, Object> cassandraConfig = new HashMap<String, Object>();
        cassandraConfig.put(StormCassandraConstants.CASSANDRA_HOST, cassandraUrl);
        cassandraConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Lists.newArrayList(keyspaceName));
        String configKey = "cassandra-config";
        topologyConfig.put(configKey, cassandraConfig);
        topologyConfig.put("smoke-test", cassandraConfig);

        return new CassandraStateFactory("smoke-test", null);
    }
}
