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
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.DestinationAlreadyExistsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;
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

    public static void setupCassandraKeySpace(String cassandraConnectionString, String keySpaceName,
                                              String columnFamily) throws ConnectionException {

        try {
            AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .forKeyspace(keySpaceName)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                    )
                    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                    .setMaxConnsPerHost(1)
                                    .setSeeds(cassandraConnectionString)
                    )
                    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            context.start();
            Keyspace keyspace = context.getClient();

            // Using simple strategy
            keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                            .put("strategy_options", ImmutableMap.<String, Object>builder()
                                    .put("replication_factor", "1")
                                    .build())
                            .put("strategy_class", "SimpleStrategy")
                            .build()
            );

            ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily.newColumnFamily(columnFamily,
                    StringSerializer.get(), StringSerializer.get());

            keyspace.createColumnFamily(CF_STANDARD1, null);
            context.shutdown();
        } catch(BadRequestException e) {
            LOG.warn("could not setup cassandra keyspace , assuming keyspace already exists.", e);
        }
    }

    public static void setupJMSQueue(String jmsConnString, String queueName) throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(jmsConnString);
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            Destination destination = session.createQueue(queueName);
        } catch(DestinationAlreadyExistsException e) {
            LOG.warn("Could not create a new queue as one already exists " + queueName, e);
        }
    }
}
