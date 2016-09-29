package cz.o2.ks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Main {

    private static final String TOPIC = "StreamsAgainTopic";

    public static void main(String[] args) throws InterruptedException {
        Properties producerConfiguration = new Properties();
        //producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mujsignal-03:9092, mujsignal-09:9093");
        producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        final KafkaProducer<String, Long> producer = new KafkaProducer<>(producerConfiguration);

        int count = 2000;
        long value = 0;

        while (count-- > 0) {
            producer.send(new ProducerRecord<>(TOPIC, 0, "Partition-0", value));
            producer.send(new ProducerRecord<>(TOPIC, 1, "Partition-1", value++));
            Thread.sleep(100);
        }

        System.out.println("Job done.");
        producer.close();
        System.exit(0);
    }
}
