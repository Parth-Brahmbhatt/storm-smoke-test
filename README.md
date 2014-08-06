storm-smoke-test
================

Smoke tests for storm with various connectors. Current code allows
users to add more smoketest by implementing the SmokeTest.java interface
and adding the implementation to list of smokeTests in TestRunner. 
There is no guice or spring integration so the code has to modified for new tests to be added.
The tests are suppose to perform all setup,cleanup and verification automatically.
Currently we have a simple test that creates following topology for storm-core and trident:

WordSpout(reads words from words.txt) -> KafkaBolt(Stores words to kafka topic)
                                      -> JmsBolt(Writes words to JMS queue)
KafkaSpout(reads word from kafkaTopic) -> WordCount(consumes the word from kafka spout and emits word,count)
JMSSpout(reads word from JMS queue) -> WordCount(consumes the word from jms spout and emits word,count)
HDFSBolt, HBaseBolt, CassandraBolt -> (Consumes word,count from WordCount bolt and stores in HDFS, HBase, Cassandra)

We have the parallelism set to 1 for sake of simplicity and in the end the verify method verifies that
word,count from file matches with HDFS files and Hbase Rows.

The test creates/deletes the kafkatopic, hdfs files and hbase tables, jms queues and cassandra key spaces.
The only thing expected out of user is to specify a properties file that
contains information about connection url for zookeeper, kafka, hbase , hdfs, cassandra and include jndi properties file 
as part of the classpath.
