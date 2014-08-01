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

package org.apache.storm.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileBasedBatchSpout implements IBatchSpout {

    private static final Logger LOG = LoggerFactory.getLogger(FileBasedBatchSpout.class);

    private String filePath;
    private Fields outputFields;
    private String separator;
    private List<String> lines;

    private int batchSize;
    private HashMap<Long, List<String>> batches = new HashMap<Long, List<String>>();
    int index = 0;
    private boolean shouldWait = true;

    public FileBasedBatchSpout(String filePath, Fields outputFields, int batchSize) {
        this(filePath, outputFields, "," , batchSize);
    }

    public FileBasedBatchSpout(String filePath, Fields outputFields, String separator, int batchSize) {
        this.filePath = filePath;
        this.outputFields = outputFields;
        this.separator = separator;
        this.batchSize = batchSize;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
        index = 0;
        try {
            this.lines = IOUtils.readLines(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        List<String> batch = this.batches.get(batchId);
        if(batch == null){

            //before emitting the first Batch wait so other spouts (cough, cough kafkaSpouts) can initialize.
            //this is a heck because currently kafka does not really support delete topic so every test runs' published
            //messages stays in the kafka topic unless someone manually deletes the topic. because of this
            //we can not set kafkaSpoutConfig.forceFromStart = true and in absence of this parameter the spout tries to
            //load offset from which it should start consuming based on where the topic is currently. If our
            //file based spout were to emit to kafka topic and the kafka spout initialized after some rows
            //were already emitted to kafka topic, our kafka spout would miss those rows. This dance is to
            //stop that from happening.
            if(shouldWait) {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    LOG.warn("sleep interrupted.", e);
                }
                shouldWait = false;
            }
            batch = new ArrayList<String>();
            while(index < lines.size()) {
                batch.add(lines.get(index++));
                if(batch.size() == batchSize) {
                    break;
                }
            }
            this.batches.put(batchId, batch);
        }
        for(String line: batch){
            collector.emit(new Values(line.split(separator)));
        }
    }

    @Override
    public void ack(long batchId) {
        this.batches.remove(batchId);
    }

    @Override
    public void close() {
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return outputFields;
    }

}
