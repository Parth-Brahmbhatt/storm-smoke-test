package org.apache.storm.helper;

/*
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

import backtype.storm.tuple.Fields;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleCassandraTridentTupleMapper implements TridentTupleMapper<String, String, String> {

    private Map<String, Class> fieldToClassMapping;
    private String keyspace;
    private String columnFamily;
    private String rowKey;

    public SimpleCassandraTridentTupleMapper(String keyspace, String columnFamily, String rowKey,  Map<String, Class> fieldToClassMapping){
        this.fieldToClassMapping = fieldToClassMapping;
        this.columnFamily = columnFamily;
        this.keyspace = keyspace;
        this.rowKey = rowKey;
    }

    @Override
    public String mapToColumnFamily(TridentTuple tuple) throws TupleMappingException {
        return columnFamily;
    }

    @Override
    public String mapToKeyspace(TridentTuple tuple) {
        return this.keyspace;
    }

    @Override
    public String mapToRowKey(TridentTuple tuple) throws TupleMappingException {
        return tuple.getStringByField(rowKey);
    }

    @Override
    public Map<String, String> mapToColumns(TridentTuple tuple) throws TupleMappingException {
        HashMap<String, String> retval = new HashMap<String, String>();
        for(String field : this.fieldToClassMapping.keySet()) {
            String value = "";
            if(fieldToClassMapping.get(field)  == String.class) {
                value = tuple.getStringByField(field);
            } else if(fieldToClassMapping.get(field) == Long.class) {
                value = String.valueOf(tuple.getLongByField(field));
            } else {
                throw new RuntimeException("SimpleCassandraTridentTupleMapper only support String and Long.");
            }
            retval.put(field, value);
        }
        return retval;
    }

    @Override
    public List<String> mapToColumnsForLookup(TridentTuple tuplex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String mapToEndKey(TridentTuple tuple) throws TupleMappingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String mapToStartKey(TridentTuple tuple) throws TupleMappingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shouldDelete(TridentTuple tuple) {
        return false;
    }

    @Override
    public Class<String> getKeyClass() {
        return String.class;
    }

    @Override
    public Class<String> getColumnNameClass() {
        return String.class;
    }

    @Override
    public Class<String> getColumnValueClass() {
        return String.class;
    }

}

