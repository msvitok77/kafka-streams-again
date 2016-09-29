package cz.o2.ks;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class Main {

    private static final String TOPIC = "StreamsAgainTopic";

    public static void main(String[] args) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-again");
        //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mujsignal-03:9092, mujsignal-09:9093");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
       // streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "mujsignal-02:2181,mujsignal-03:2182");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Long> numbers = builder.stream(stringSerde, longSerde, TOPIC);
        numbers.map((key, value) -> new KeyValue<Object, Object>(key, value * 2)).print();

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
