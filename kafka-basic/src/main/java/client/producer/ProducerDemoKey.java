package client.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ProducerDemoKey {

    private static final String bootstrapServers = "127.0.0.1:9092";

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoKey.class);

    private static final Properties properties = new Properties();

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static void main(String[] args) {

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        final String topic = "my-topic";
        final String message = "hello world";
        final String key = "id_";

        IntStream.range(0,10).forEach(
                i -> sendMessage(producer, topic, key + i,message + i)
        );

        producer.flush();
        producer.close();
    }

    private static void sendMessage(final KafkaProducer<String, String> producer,
                                    final String topic,
                                    final String key,
                                    final String message) {

        try {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    print(metadata);
                } else {
                    print(exception);
                }
            }).get(); //synchronous
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void print(RecordMetadata metadata) {
        logger.info("metadata\n" +
                "topic:"+ metadata.topic() +"\n" +
                "partition:"+ metadata.partition() +"\n" +
                "offset:"+ metadata.offset() +"\n" +
                "timestamp:"+ metadata.timestamp());
    }

    private static void print(Exception e) {
        logger.error("error", e);
    }

}
