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

package org.apache.storm.verify;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VerifyUtils {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyUtils.class);
    public static final String FIELD_SEPERATOR = ",";

    public static void verifyHdfs(String hdfsUrl, String dir, List<String> expectedLines) throws Exception {
        List<String> lines = new ArrayList<String>();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUrl), new Configuration());
        Path path = new Path(dir);
        assert fileSystem.exists(path);
        assert fileSystem.isDirectory(path);
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        assert fileStatuses != null;
        for(FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            InputStreamReader is = new InputStreamReader(fileSystem.open(filePath));
            lines.addAll(IOUtils.readLines(is));
        }

        Collections.sort(lines);
        Collections.sort(expectedLines);

        assert lines.equals(expectedLines) : "expectedLines = " + expectedLines + " actualines = " + lines;
    }

    public static void verifyHbase(String hBaseUrl, String tableName, String columnFamily,
                                   Map<String, Class> columnNamesToTypes, List<String> expectedRows) throws Exception {
        List<String> lines = new ArrayList<String>();
        final Configuration hbConfig = HBaseConfiguration.create();
        hbConfig.set("hbase.rootdir", hBaseUrl);
        HTable table = new HTable(hbConfig, tableName);
        ResultScanner results = table.getScanner(columnFamily.getBytes());
        for (Result result = results.next(); (result != null); result = results.next()) {
            String line = "";
            for(Map.Entry<String, Class> columnNameAndType : columnNamesToTypes.entrySet()) {
                byte[] bytes = result.getValue(columnFamily.getBytes(), columnNameAndType.getKey().getBytes());
                String colVal = toString(bytes, columnNameAndType.getValue());
                line += colVal + FIELD_SEPERATOR;
            }
            line = StringUtils.removeEnd(line, FIELD_SEPERATOR);
            lines.add(line);
        }

        Collections.sort(lines);
        Collections.sort(expectedRows);
        assert lines.equals(expectedRows) : "expectedRows = " + expectedRows + " actualRows = " + lines;
    }

    public static void verifyCassandra(String cassandraUrl, String keyspaceName, String columnFamily,
                                       final Map<String, Class> columnNamesToTypes,
                                       List<String> expectedRows) throws Exception {
        ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily.newColumnFamily(columnFamily,
                StringSerializer.get(), StringSerializer.get());

        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                .setMaxConnsPerHost(1)
                                .setSeeds(cassandraUrl)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getClient();

        final List<String> actualLines = Lists.newArrayList();
        boolean result = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(100) // Read 100 rows at a time
                .withConcurrencyLevel(1) // just use one thread.
                .withPartitioner(null) // this will use keyspace's partitioner
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(@Nullable Row<String, String> row) {
                        ColumnList<String> columns = row.getColumns();
                        String line = "";
                        for(Map.Entry<String, Class> entry : columnNamesToTypes.entrySet()) {
                            String columnName = entry.getKey();
                            line += VerifyUtils.toString(columns.getByteArrayValue(columnName, null), entry.getValue());
                            line += FIELD_SEPERATOR;
                        }
                        actualLines.add(StringUtils.removeEnd(line, FIELD_SEPERATOR));
                        return true;
                    }
                })
                .build()
                .call();

        Collections.sort(actualLines);
        Collections.sort(expectedRows);
        assert actualLines.equals(expectedRows) : "expectedRows = " + expectedRows + " actualRows = " + actualLines;
    }

    public static String toString(byte[] bytes, Class type)  {
        if(String.class.equals(type)) {
            return new String(bytes);
        } else if (Long.class.equals(type)) {
            return String.valueOf(Longs.fromByteArray(bytes));
        } else if (Integer.class.equals(type)) {
            return String.valueOf(Ints.fromByteArray(bytes));
        } else  {
           throw new IllegalArgumentException("currently we only support String, Long and Integer types");
        }
    }

}
