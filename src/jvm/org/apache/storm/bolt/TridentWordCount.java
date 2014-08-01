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

package org.apache.storm.bolt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Simple wordCount function that keeps an in memory state for each word an corresponding count.
 */
public class TridentWordCount implements Function {
    private Map<String, Long> wordCountMap;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if(tuple.getValues().isEmpty()) return;
        String word = new String(tuple.getBinary(0));
        Long count = 1l;
        if(wordCountMap.containsKey(word)) {
            count += wordCountMap.get(word);
        }
        wordCountMap.put(word, count);
        collector.emit(Lists.<Object>newArrayList(word, count));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        wordCountMap = Maps.newHashMap();
    }

    @Override
    public void cleanup() {

    }
}
