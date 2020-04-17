import com.sun.tools.javac.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

@Slf4j
public class KafkaConsumerGroupApp2 {
    public static void main(String[] args) {
        System.out.println("Starting KafkaConsumerGroupApp2");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test-group");

        try (KafkaConsumer consumer = new KafkaConsumer(props)) {
            List<String> topics = List.of("big-topic");
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Topic: {}, Partition: {}, Offset: {}, key: {}, value: {}", record.topic(), record.partition(), record.offset(), record.key(), record.value().toUpperCase());
                }
            }

        } catch (Exception e) {
            log.error("Consumer error", e);
        }
    }
}
