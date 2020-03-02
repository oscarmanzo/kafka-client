package com.kafka.client.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        final String bootstrapServers = "127.0.0.1:9092";
        final String topic = "my-topic";

        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello world");

        //producer.send(record);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                logger.info("metadata\n" +
                        "topic:"+ metadata.topic() +"\n" +
                        "partition:"+ metadata.partition() +"\n" +
                        "offset:"+ metadata.offset() +"\n" +
                        "timestamp:"+ metadata.timestamp());
            } else {
                logger.error("error", exception);
            }
        });

        producer.flush();
        producer.close();
    }

}
