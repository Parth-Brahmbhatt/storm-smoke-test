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
package org.apache.storm.helper;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.tuple.Tuple;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

public class SimpleJmsProducer implements JmsMessageProducer{

    @Override
    public Message toMessage(Session session, Tuple tuple) throws JMSException {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(tuple.getString(0));
        return message;
    }
}
