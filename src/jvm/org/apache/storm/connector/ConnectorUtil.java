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

import backtype.storm.tuple.Fields;
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
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

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
        return new KafkaSpout(spoutConfig);
    }

    public static HdfsBolt getHdfsBolt(String fsUrl, String srcDir, String rotationDir) {
        // sync the filesystem after every tuple
        SyncPolicy syncPolicy = new CountSyncPolicy(1);

        // rotate files when they reach 1KB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(0.001f, FileSizeRotationPolicy.Units.KB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(srcDir)
                .withExtension(".txt");

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(fsUrl)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
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


}
