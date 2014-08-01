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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class FileBasedSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(FileBasedSpout.class);

    private BufferedReader reader;
    private SpoutOutputCollector _collector;
    private String filePath;
    private Fields outputFields;
    private String separator;
    private boolean shouldWait = true;

    public FileBasedSpout(String filePath, Fields outputFields) {
        this(filePath, outputFields, ",");
    }

    public FileBasedSpout(String filePath, Fields outputFields, String separator) {
        this.filePath = filePath;
        this.outputFields = outputFields;
        this.separator = separator;
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
        InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
        this.reader = new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        String line = null;
        try {
            //before emitting the first tuple wait so other spouts (cough, cough kafkaSpouts) can initialize.
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
            if ((line = this.reader.readLine()) != null) {
                _collector.emit(new Values(line.split(separator)));
            } else {
                LOG.debug("all lines have been processed.");
            }
        } catch (Exception e) {
            LOG.warn("failed to emit a tuple", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outputFields);
    }
}
