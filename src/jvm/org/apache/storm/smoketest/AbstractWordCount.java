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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.storm.cleanup.CleanupUtils;
import org.apache.storm.setup.SetupUtils;
import org.apache.storm.verify.VerifyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Simple test that create a topology as following:
 * WordSpout(reads words from words.txt) -> KafkaBolt(Stores words to kafka topic)
 * KafkaSpout(reads word from kafkaTopic) -> WordCount(consumes the word from kafka spout and emits word,count)
 * HDFSBolt,HBaseBole -> (Consumes word,count from WordCount bolt and stores in HDFS and HBase)
 *
 * We have the parallelism set to 1 for sake of simplicity and in the end the verify method verifies that
 * word,count from file matches with HDFS files and Hbase Rows.
 */
public abstract class AbstractWordCount implements  SmokeTest {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractWordCount.class);

    protected String zkConnString;

    protected String hdfsUrl;

    protected String hbaseUrl;

    protected String kafkaBrokerlist;

    public AbstractWordCount(String zkConnString, String kafkaBrokerlist, String hdfsUrl, String hbaseUrl) {
        this.zkConnString = zkConnString;
        this.hdfsUrl = hdfsUrl;
        this.hbaseUrl = hbaseUrl;
        this.kafkaBrokerlist = kafkaBrokerlist;
    }

    @Override
    public void setup() throws Exception {
        SetupUtils.createKafkaTopic(this.zkConnString, getTopicName());
        SetupUtils.createHBaseTable(this.hbaseUrl, getTableName(), getColumnFamily());
    }

    @Override
    public void cleanup() throws Exception {
        CleanupUtils.deleteHBaseTable(this.hbaseUrl, getTableName());
        CleanupUtils.deleteHdfsDirs(this.hdfsUrl, Lists.newArrayList(getHDfsSrcDir(), getHDfsDestDir()));
        CleanupUtils.deleteKafkaTopic(this.zkConnString, getTopicName());
    }

    @Override
    public long getAllowedExecutionMills() throws Exception {
        return 60000l;
    }

    @Override
    public void verify() throws Exception {
        List<String> expectedLines = getWordCount();
        Map<String, Class> hbaseColNameToTypeMap = Maps.newLinkedHashMap();
        hbaseColNameToTypeMap.put("word", String.class);
        hbaseColNameToTypeMap.put("count", Long.class);
        VerifyUtils.verifyHdfs(this.hdfsUrl, getHDfsSrcDir(), expectedLines);
        VerifyUtils.verifyHbase(this.hbaseUrl, getTableName(), getColumnFamily(), hbaseColNameToTypeMap, expectedLines);

        LOG.info("verification succeeded.");
    }

    private List<String> getWordCount() throws Exception {

        List<String> words = IOUtils.readLines(getClass().getClassLoader().getResourceAsStream("words.txt"));
        Map<String, Long> wordCountMap = Maps.newHashMap();
        for (String word : words) {
            Long count = wordCountMap.get(word);
            if (count == null) {
                count = 0l;
            }
            wordCountMap.put(word, count + 1);
        }

        List<String> wordCounts = Lists.newArrayList();
        for (Map.Entry<String, Long> entry : wordCountMap.entrySet()) {
            wordCounts.add(entry.getKey() + "," + entry.getValue());
        }

        return wordCounts;
    }

    public abstract String getTopicName();
    public abstract String getTableName();
    public abstract String getColumnFamily();
    public abstract String getHDfsSrcDir();
    public abstract String getHDfsDestDir();

}
