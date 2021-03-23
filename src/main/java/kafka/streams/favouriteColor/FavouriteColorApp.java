package kafka.streams.favouriteColor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {
    //    Start the server
    // bin/zookeeper-server-start.sh config/zookeeper.properties
    // bin/kafka-server-start.sh config/server-1.properties
    // bin/kafka-server-start.sh config/server-2.properties
    // bin/kafka-server-start.sh config/server-3.properties


    //Problem:
//    input:
//    stephan,blue
//    john,green
//    stephan,red
//    alice,red

//    Crete the topics
//    input topic:
//    bin/kafka-topics.sh --create --topic favourite-color-input --zookeeper localhost:2181 --replication-factor 1 --partitions 1
//    Create intermediate log compact topic (compact topic  is just an optimization, it won't change the result)
//    bin/kafka-topics.sh --create --topic user-keys-and-colors --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --config cleanup.policy=compact
//    Create intermediate log compact topic (compact topic  is just an optimization, it won't change the result)
//    bin/kafka-topics.sh --create --topic favourite-color-output --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --config cleanup.policy=compact

//     View List of topics
//    bin/kafka-topics.sh --list --zookeeper localhost:2181

//    Launch a Kafka consumer
//    bin/kafka-console-consumer.sh --topic favourite-color-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer  --bootstrap-server localhost:9092

//    Launch a Kafka Producer to accept in put
//    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic favourite-color-input
//    input:
//    stephan,blue
//    john,green
//    stephan,red
//    alice,red

    public static void main(String[] args) {
        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application"); //group.id
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Only set this to 0 when doing development. Disable the cache to demonstrate all the steps involved in transformation - dont do it in prod
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        //Read from Kafka stream
        KStream<String, String> textLines = builder.stream("favourite-color-input");

        //Step1 : Crate a topic of user keys and colors eg: key=stephan value=red
        KStream<String, String> usersAndColors = textLines
                //Ignore bad values
                .filter((key, value) -> value.contains(","))
                //Make the user as the key
                .selectKey((key, value) -> value.split(",")[0].toUpperCase())
                //Make the color as the value
                .mapValues(value -> value.split(",")[1].toUpperCase())
                //Make sure the color is in the allowed color list
                .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

        usersAndColors.to("user-keys-and-colors");

        //Step2: we read the topic as a KTable so that updates are read correctly
        KTable<String, String> userAndColorsTable = builder.table("user-keys-and-colors");

        //Step: Count the occurences of the colors
        KTable<String, Long> favouriteColor = userAndColorsTable
                //Group by colors and then count. Creating color as the key eg red: 2
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();

        favouriteColor.toStream().to("favourite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        //Only do this in dev not in prod
        streams.cleanUp();
        streams.start();

        //Print the topology
        System.out.println(streams.toString());

        //Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
