package kafka.streams.bankBalance;

import kafka.streams.bankBalance.serializer.JsonDeserializer;
import kafka.streams.bankBalance.serializer.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BankBalanceExactlyOnceApp {
    //    Crete the topics
//    bin/kafka-topics.sh --create --topic bank-transactions --zookeeper localhost:2181 --replication-factor 1 --partitions 1
//    bin/kafka-topics.sh --create --topic bank-balance-exactly-once --zookeeper localhost:2181 --replication-factor 1 --partitions 1

//     View List of topics
//    bin/kafka-topics.sh --list --zookeeper localhost:2181

//    Start your consumer for bank-transactions
    //    bin/kafka-console-consumer.sh --topic bank-transactions --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer  --bootstrap-server localhost:9092

//    Now start the BankTransactionProducer (You will see the producer writing to the topic)

//    Start the consumer for finla topic bank-balance-exactly-once
    //    bin/kafka-console-consumer.sh --topic bank-balance-exactly-once --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer  --bootstrap-server localhost:9092

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application"); //group.id
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Only set this to 0 when doing development. Disable the cache to demonstrate all the steps involved in transformation - dont do it in prod
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        JsonSerializer<Transaction> transactionJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Transaction> transactionJsonDeserializer = new JsonDeserializer<>(Transaction.class);
        Serde<Transaction> transactionSerde = Serdes.serdeFrom(transactionJsonSerializer, transactionJsonDeserializer);


        JsonSerializer<Balance> balanceJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Balance> balanceJsonDeserializer = new JsonDeserializer<>(Balance.class);
        Serde<Balance> balanceSerde = Serdes.serdeFrom(balanceJsonSerializer, balanceJsonDeserializer);


        StreamsBuilder builder = new StreamsBuilder();

        Balance initialBalance = Balance.builder()
                .count(0)
                .balance(0)
                .time(Instant.ofEpochMilli(0L).toString())
                .build();

        //Read from Kafka stream
        KStream<String, Transaction> bankTransactions = builder.stream("bank-transactions", Consumed.with(Serdes.String(), transactionSerde));

        KTable<String, Balance> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, value, aggregate) -> newBalance(value, aggregate),
                        Materialized.as("bank-total-balances").with(Serdes.String(), balanceSerde)
                );

        bankBalance.toStream().to("bank-balance-exactly-once", Produced.with(Serdes.String(), balanceSerde));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-bank-exactly-once") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }

    private static Balance newBalance(Transaction transaction, Balance balance) {
        long newBalance = balance.getBalance() + transaction.getAmount();
        Long balanceEpoch = Instant.parse(balance.getTime()).toEpochMilli();
        Long transactionEpoch = transaction.getTime().toEpochSecond();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));

        return Balance.builder()
                .balance(newBalance)
                .count(balance.getCount() + 1)
                .time(newBalanceInstant.toString())
                .build();
    }
}
