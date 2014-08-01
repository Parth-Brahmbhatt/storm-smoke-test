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

package org.apache.storm.setup;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Setup utilities for dependencies.
 */
public class SetupUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SetupUtils.class);

    public static void createKafkaTopic(String zkConnString, String topicName) {
        final int sessionTimeoutMillis = 10000;
        final int connTimeOutMillis = 10000;
        final int noOfPartitions = 1;
        final int replicationFactor = 1;
        ZkClient zkClient = new ZkClient(zkConnString, sessionTimeoutMillis, connTimeOutMillis,  ZKStringSerializer$.MODULE$);

        try {
            AdminUtils.createTopic(zkClient, topicName, noOfPartitions, replicationFactor, new Properties());
        } catch(Exception e) {
            LOG.warn("could not create topic " + topicName + " assuming that it already exists and moving on.");
        }
    }

    public static void createHBaseTable(String hbaseUrl, String tableName, String... columnFamilies) throws IOException {
        final Configuration hbConfig = HBaseConfiguration.create();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(hbConfig);
        hbConfig.set("hbase.rootdir", hbaseUrl);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String familyName:  columnFamilies) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(familyName);
            tableDescriptor.addFamily(columnDescriptor);
        }

        TableName[] tableNames = hBaseAdmin.listTableNames();
        for(TableName table : tableNames)  {
            if(table.getNameAsString().equals(tableName)) {
                return;
            }
        }
        hBaseAdmin.createTable(tableDescriptor);
    }

}
