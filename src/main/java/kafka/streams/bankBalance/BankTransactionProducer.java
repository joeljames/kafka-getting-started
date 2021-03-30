package kafka.streams.bankBalance;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.OffsetDateTime;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class BankTransactionProducer {
    private static Gson gson = GsonFactory.create();

    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //producer ack
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //Strongest producing guarantee, but bit slow (Depends on use case)(All nodes should ack)
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1"); // Use only in dev not prod

        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //Ensures we dont push duplicates

        try (KafkaProducer<String, String> producer = new KafkaProducer(props)) {
            int i = 0;

            while (true) {
                log.info("Producing batch: {}", i);
                try {
                    producer.send(newRandomTransaction("john"));
                    Thread.sleep(100);

                    producer.send(newRandomTransaction("smith"));
                    Thread.sleep(100);

                    producer.send(newRandomTransaction("peter"));
                    Thread.sleep(100);
                    i++;
                } catch (InterruptedException ex) {
                    log.info("Shutting down at batch {}", i);
                    break;
                }
            }

        } catch (Exception e) {
            log.error("Failed to send message by the producer", e);
        }

    }

    private static ProducerRecord<String, String> newRandomTransaction(String name) {
        OffsetDateTime now = OffsetDateTime.now();
        int amount = ThreadLocalRandom.current().nextInt(0, 100);

        Transaction transaction = new Transaction(name, amount, now);
        String stringifiedTransaction = gson.toJson(transaction);
        // key = name
        return new ProducerRecord<>("bank-transactions", name, stringifiedTransaction);
    }
}
