import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class KafkaProducerApp {
    private static final String topic = "my-topic";

    public static void main(String[] args) {
        System.out.println("Starting the producer main application");

        // Kafka Producer required properties
        //bootstrap.servers: Producer uses this to determine the cluster membership, partitions, leaders etc. You dont have to specify all the servers, but it's best practice to specify more than one broker, incase if the specified broker is unavailable the producer will use the next one.
        //key.serializer: class used for message serialization. This is to optimize the size of the messages not only for network transmission, but for storage and even compression
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // You can also send to a particular partition along with unix timestamp.
        // Not recommended in high scale env as addition bytes have to be sent over via network.
        // Broker logs it's own timestamp
        // ProducerRecord<Object, String> message1 = new ProducerRecord<>(topic, 1, Instant.now().toEpochMilli(), "Message 1");


        try (KafkaProducer<String, String> producer = new KafkaProducer(props)) {
            int counter = 0;

            while (counter <= 100) {
                // When calling the send method the producer will reach out to the cluster using the bootstrap.servers list set above
                // to discover the cluster membership. The response comes back as metadata, containing info about the
                // topics, partitions and managing brokers on the cluster.
                // Producer will use this metatdata instance through out it's life cycle and keep it up to date.

                //Data flow when the send call is invoked
                // send() ->
                // serializer ->
                // partitioner (decides which partition to send the message) (Strategies: direct, round-robin, key-modhash, custom)
                // record accumulator (Batches the record which are going to be sent to broker, does not send one at a time. Can be configured in producer properties)
                // broker
                // get back a RecordMetadata

                //Producer can also set the level of acks it should receive from the broker when sending a message
                //1) 0 -> Fire and forget (fastest not reliable)
                //2) 1 -> Only the leader broker to ack, instead of all the brokers in the replica to confirm
                //3) 2 -> all in sync replica should confirm (slow performance)

                // When broker responds with error
                //1) retries
                //2) retry.backoff.ms // wait period in ms between retries

                // If your application requires message ordering
                // You can only have ordering if you are sending messages through a single partition
                // With multiple partitions you have to maintain the order at the consumer level. Errors can complicate this too eg retry.backoff.ms.
                // At a very high performance cost you can set max.inflight.request.per.connection (meaning a producer can only send one message at a time)

                //Message delivery assurance:
                //1) at most once
                //2) at least once
                //3) only once
                //Use key if you want all the messages to go to a single partition (This will ensure ordering of data)
//                ProducerRecord<String, String> message1 = new ProducerRecord<>(topic, "key", msg);
                String msg = "Message " + counter;
                ProducerRecord<String, String> message1 = new ProducerRecord<>(topic, msg);

                producer.send(message1);
                log.info("Sent message: " + msg);
                counter++;
            }

        } catch (Exception e) {
            log.error("Failed to send message by the producer", e);
        }
    }
}
