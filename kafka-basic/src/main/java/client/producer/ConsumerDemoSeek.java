package client.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoSeek {

    private static final String bootstrapServers = "127.0.0.1:9092";
    private static final String resetOffset = "earliest";

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoSeek.class);
    private static final Properties properties = new Properties();

    static {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetOffset);
    }

    public static void main(String[] args) {

        final String topic = "my-topic";
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // define the partition
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        final long offsetFrom = 15L; // define the offset for start reading
        consumer.seek(topicPartition, offsetFrom);

        int numToRead = 5;
        int i = 0;
        boolean reading = true;

        while (reading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                i++;
                print(record);

                if (i >= numToRead){
                    reading = false;
                    break;
                }
            }
        }

    }

    private static void print(ConsumerRecord record) {
        logger.info("key:"+ record.key() +" ");
        logger.info("value:"+ record.value() +" ");
        logger.info("partition:"+ record.partition() +" ");
        logger.info("offset:"+ record.offset() +"\n");
    }

}
