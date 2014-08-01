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

package org.apache.storm.cleanup;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Utility cleanup method.
 */
public class CleanupUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CleanupUtils.class);

    public static void deleteKafkaTopic(String zkConnString, String topicName) {
        final int sessionTimeoutMillis = 10000;
        final int connTimeOutMillis = 10000;
        ZkClient zkClient = new ZkClient(zkConnString, sessionTimeoutMillis, connTimeOutMillis, ZKStringSerializer$.MODULE$);

        try {
            AdminUtils.deleteTopic(zkClient, topicName);
        } catch (Exception e) {
            LOG.warn("could not delete topic " + topicName + " assuming that it does not exist and moving on.", e);
        }
    }

    public static void deleteHdfsDirs(String hdfsUrl, List<String> dirs) throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUrl), new Configuration());
        for (String dir : dirs) {
            Path path = new Path(dir);
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
        }
    }

    public static void deleteHBaseTable(String hbaseUrl, String tableName) throws Exception {
            final Configuration hbConfig = HBaseConfiguration.create();
            hbConfig.set("hbase.rootdir", hbaseUrl);
            HBaseAdmin hBaseAdmin = new HBaseAdmin(hbConfig);
            HTableDescriptor[] tables = hBaseAdmin.listTables();
            for (HTableDescriptor table : tables) {
                if (table.getNameAsString().equals(tableName)) {
                    hBaseAdmin.disableTable(tableName);
                    hBaseAdmin.deleteTable(tableName);
                }
            }
    }
}
