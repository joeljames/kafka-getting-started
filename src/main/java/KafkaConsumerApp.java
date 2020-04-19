import com.sun.tools.javac.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

@Slf4j
public class KafkaConsumerApp {
    private static final String DEMO_TOPIC_1 = "topic-1";
    private static final String DEMO_TOPIC_2 = "topic-2";

    public static void main(String[] args) {
        System.out.println("Starting the consumer main application");

        //Consumer configurations https://kafka.apache.org/documentation.html#consumerconfigs
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("group.id", "test");

        //Consumers are long running application which polls for messages from the brokers and process it.
        //Consumer can subscribe to any number of topics. 1 to infinity
        //Subscribe call does is it pull messages from multiple topics and multiple partitions
        //Subscribe using regex: consumer.subscribe(List.of("demo-*"));
        try (KafkaConsumer consumer = new KafkaConsumer(props)) {
            List<String> topics = List.of(DEMO_TOPIC_1, DEMO_TOPIC_2);
            int timeoutMilliSecs = 10;
            consumer.subscribe(topics);
            while (true) {
                //poll is single threaded
                // poll -> fetcher -> broker //keeps the tcp connection open until timeout
                //fetcher buffers the records -> deserializes it
                ConsumerRecords<String, String> records = consumer.poll(timeoutMilliSecs);
                for (ConsumerRecord<String, String> record : records) {
                    //Process the record
                    //Note this is single threaded, so if you spend too much time processing the record it could have big implications on the env in which consumer process is running
                    //Slow consumer wont have impact on producer, brokers or other consumers
                    log.info("Topic: {}, Partition: {}, Offset: {}, key: {}, value: {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                //Offset: Consumers tracks the offset, to determine what it has read
                //Kafka store the committed offset in a special topic called __consumer_offsets
                //
                //read != committed
                //offset commit behaviour is configurable
                //enable.auto.commit = true (default) kafka will manage you commits for you
                //If you know how long you consumer will take to process records the you can set below config for commit
                //auto.commit.interval.ms = 5000

                //Commit after processing the record batch to move the offset. Blocking call, does reties automatically
                //consumer.commitSync();
//                consumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                        //do something
//                    }
//                });
            }

        } catch (Exception e) {
            log.error("Consumer error", e);
        }
    }
}
