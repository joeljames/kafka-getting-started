package kafka.streams.wourdCount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
//    Start the server
    // bin/zookeeper-server-start.sh config/zookeeper.properties
    // bin/kafka-server-start.sh config/server-1.properties
    // bin/kafka-server-start.sh config/server-2.properties
    // bin/kafka-server-start.sh config/server-3.properties

//    Crete the topics
//    bin/kafka-topics.sh --create --topic word-count-input --zookeeper localhost:2181 --replication-factor 3 --partitions 3
//    bin/kafka-topics.sh --create --topic word-count-output --zookeeper localhost:2181 --replication-factor 3 --partitions 3

//     View List of topics
//    bin/kafka-topics.sh --list --zookeeper localhost:2181

//    Start producer on the CLI and send data
//    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic word-count-input

    //Listen to the output channel and print the result
//    bin/kafka-console-consumer.sh --topic word-count-output --from-beginning --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer  --bootstrap-server localhost:9092

    public static void main(String[] args) {
        Properties props = new Properties();
        //Never change the application id
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application"); //group.id
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //By default it reads from the latest (If an application is starting for the first time)
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        //Read from Kafka stream
        //Input: 
        //Kafka kafka KAFKA
        //Kafka is my fav
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        //KTable new record will be a upsert if values is not null, else delete if values is null
        //builder.table("word-count-input")
        KTable<String, Long> counts = wordCountInput
                //mapValues can be used for both KStreams and Ktable. It does not change the key. Does not trigger re-partition
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                //Flat map values and split by space
                // Takes 1 and produces 0, 1 or more Use flatMap when you want to take one record and split it into smaller values
                .flatMapValues(v -> Arrays.asList(v.split(" ")))
                //Select key to apply a key
                .selectKey((ignoreKey, word) -> word)
                //Before agg, group by key
                .groupByKey()
                //Count occurance
                .count();

        //To is a terminal operator. Write to a new topic stream.to() or table.to()
        // need to override value serde to Long type
        counts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        //Print the topology
        System.out.println(streams.toString());

        //Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
