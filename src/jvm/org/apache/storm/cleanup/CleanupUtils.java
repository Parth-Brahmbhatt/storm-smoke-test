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

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
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
                    try {
                        hBaseAdmin.disableTable(tableName);
                    } catch (TableNotEnabledException e) {
                        LOG.info(tableName + " failed to disable as it was never enabled", e);
                    }
                    hBaseAdmin.deleteTable(tableName);
                }
            }
    }

    public static void deleteCassandraKeySpace(String cassandraConnString, String keySpace) throws Exception {

        try {
            AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .forKeyspace(keySpace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                    )
                    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                    .setMaxConnsPerHost(1)
                                    .setSeeds(cassandraConnString)
                    )
                    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            context.start();
            Keyspace keyspace = context.getClient();
            keyspace.dropKeyspace();
            context.shutdown();
        } catch (BadRequestException e) {
            LOG.warn("Could not delete cassandra keyspace, assuming it does not exist.", e);
        }
    }

    public static void deleteQueue(String jmsConnString, String queueName) throws Exception {

    }
}
