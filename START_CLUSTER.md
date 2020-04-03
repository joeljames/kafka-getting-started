# Installing Kafka and starting single broker Kafka
    1. Scala is required for kafka to run. Kafka is written in scala
        1. brew install scala
        
    2. Create directory /usr/local/bin/kafka
    
    3. Download Kafka:
        1. https://kafka.apache.org/downloads
        
    4. Copy ove the downloaded uncompressed to /usr/local/bin/kafka
        1. cp -r kafka_2.13-2.4.1 /usr/local/bin/kafka
        2. kafka_2.13-2.4.1/bin
        
    5. Start zookeeper 
        1. bin/zookeeper-server-start.sh config/zookeeper.properties
        
    6. Test connecting to zookeeper 
        1. telnet localhost 2181
        
    7. Start Broker (kafka server)
        1. bin/kafka-server-start.sh config/server.properties
        
    8. Create topic (zookeeper scans its registry of brokers and made a decision to assign a broker as a leader for topic “my_topic")
        1. bin/kafka-topics.sh --create --topic my_topic --zookeeper localhost:2181 --replication-factor 1 --partitions 1
        2. You will see Created log for partition my_topic-0 in /tmp/kafka-logs/my_topic-0
        
    9. View list of topics 
        1. bin/kafka-topics.sh --list --zookeeper localhost:2181
        
    10. Start the producer
        1.  bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my_topic
        2. Now type message and the producer will send the message to broker and the broker will commit it to the log. Now the consumer can consume a topic 
        
    11. Start the consumer 
        1. bin/kafka-console-consumer.sh  --topic my_topic --from-beginning --bootstrap-server localhost:9092

# Starting Kafka with 3 broker and 1 partition 
    Kafka is resilient even if a broker goes down when replication factor is more than 1
    you can still publish subscribe.
    1. Create 3 separate server-1.properties configuration files
        1. cp server.properties server-1.properties
    2. Change the setting value under  server-{0,1,2}.properties for 
        1. log.dirs to be unique to avoid lock conflicts
        2. broker.id to be unique
        3. listeners=PLAINTEXT://localhost:9093 to be unique
    3. Start zookeeper
        1. bin/zookeeper-server-start.sh config/zookeeper.properties
    4. Start 3 Broker (kafka server)
        1. bin/kafka-server-start.sh config/server-0.properties
        2. bin/kafka-server-start.sh config/server-1.properties
        3. bin/kafka-server-start.sh config/server-2.properties
    5. Create topic 
        1. bin/kafka-topics.sh --create --topic replicated_topic --zookeeper localhost:2181 --replication-factor 3 --partitions 1
    6. Check details about the topic 
        1. bin/kafka-topics.sh --describe —topic replicated_topic --zookeeper localhost:2181
    7. Start the producer
        1.  bin/kafka-console-producer.sh --broker-list localhost:9092,  localhost:9093 --topic replicated_topic
    8. Start the consumer 
        1. bin/kafka-console-consumer.sh  --topic replicated_topic --from-beginning --bootstrap-server localhost:9092
    9. Now, lets replicate the broker failure. Kill the broker mentioned as leader in step 5.
        1. Run bin/kafka-topics.sh --describe —topic replicated_topic --zookeeper localhost:2181
        2. You will see Leader changed and Replicas will be 0,1,2 but ISR will be 1,2
        3. You will still be able to publish and consume messages because you still have 2 other brokers running and working
        
# Starting Kafka with 3 broker and 3 partition 
    1. Create 3 separate server-1.properties configuration files
        1. cp server.properties server-1.properties
    2. Change the setting value under  server-{0,1,2}.properties for 
        1. log.dirs to be unique to avoid lock conflicts
        2. broker.id to be unique
        3. listeners=PLAINTEXT://localhost:9093 to be unique
    3. Start zookeeper
        1. bin/zookeeper-server-start.sh config/zookeeper.properties
    4. Start 3 Broker (kafka server)
        1. bin/kafka-server-start.sh config/server-0.properties
        2. bin/kafka-server-start.sh config/server-1.properties
        3. bin/kafka-server-start.sh config/server-2.properties
    5. Create topic 
        1. bin/kafka-topics.sh --create --topic my-topic-demo --zookeeper localhost:2181 --replication-factor 3 --partitions 3   
    6. Check details about the topic 
        1. bin/kafka-topics.sh --describe —topic my-topic-demo --zookeeper localhost:2181   
        ```bash
                Topic: my-topic-demo	PartitionCount: 3	ReplicationFactor: 3	Configs:
                Topic: my-topic-demo	Partition: 0	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
                Topic: my-topic-demo	Partition: 1	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
                Topic: my-topic-demo	Partition: 2	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1
        ```   
    8. Start the consumer 
        1. bin/kafka-console-consumer.sh  --topic my-topic-demo --bootstrap-server localhost:9092          