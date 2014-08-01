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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
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

import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VerifyUtils {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyUtils.class);

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

    public static void verifyHbase(String hBaseUrl, String tableName, String columnFamily, Map<String, Class> columnNamesToTypes, List<String> expectedRows) throws Exception {
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
                line += colVal + ",";
            }
            line = StringUtils.removeEnd(line, ",");
            lines.add(line);
        }

        Collections.sort(lines);
        Collections.sort(expectedRows);
        assert lines.equals(expectedRows) : "expectedRows = " + expectedRows + " actualRows = " + lines;
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
