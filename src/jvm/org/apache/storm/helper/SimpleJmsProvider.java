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

import backtype.storm.contrib.jms.JmsProvider;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

public class SimpleJmsProvider implements JmsProvider {
    private ConnectionFactory connectionFactory;

    private Destination destination;

    public SimpleJmsProvider(String connString, String queueName) throws Exception {
        this.connectionFactory = new ActiveMQConnectionFactory(connString);
//        Connection connection = connectionFactory.createConnection();
//        connection.start();
//        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        Destination destination = session.createQueue(queueName);

        Context jndiContext = new InitialContext();
        this.destination = (Destination) jndiContext.lookup(queueName);
    }


    @Override
    public ConnectionFactory connectionFactory() throws Exception {
        return this.connectionFactory;
    }

    @Override
    public Destination destination() throws Exception {
        return this.destination;
    }
}
