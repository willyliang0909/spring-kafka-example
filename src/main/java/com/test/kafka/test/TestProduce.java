package com.test.kafka.test;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class TestProduce {

    public static void main(String[] args) throws InterruptedException {
        final String bootstrapServers = "localhost:9092";
        Properties properties = new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            put(ProducerConfig.ACKS_CONFIG, "1");
        }};

        Producer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record
                = new ProducerRecord<>("test_topic", "key", "value");


        Callback callback = (recordMetadata, e) -> {
            if (e != null) {
                System.out.println(e);
            } else {
                System.out.println("topic: " + recordMetadata.topic() +
                        " partition: " + recordMetadata.partition() +
                        " offset: " + recordMetadata.offset());
            }
        };

        // use the callback with the `send` method...
        Future<RecordMetadata> result = producer.send(record, callback);

        while (true) {
            Thread.sleep(3000);
        }


    }
}
