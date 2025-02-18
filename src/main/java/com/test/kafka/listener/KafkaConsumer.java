package com.test.kafka.listener;

import com.test.kafka.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumer {

    @KafkaListener(topics = "test_topic")
    public void consume(String message) {
        log.info("Consumed message: {}", message);
    }

    @KafkaListener(topics = "user_topic", containerFactory = "userKafkaListenerFactory")
    public void consumeUser(User user) {
        log.info("Consumed user: {}", user);
    }

    @KafkaListener(topics = "user_topic", groupId = "spring-user", containerFactory = "userKafkaListenerFactory")
    public void consumeUserDetail(ConsumerRecord<String, User> record)  {
        log.info("------------------");
        log.info("Consumed message: {}, from partition: {}, offset: {}, key: {}, timestamp: {}",
                record.value(),
                record.partition(),
                record.offset(),
                record.key(),
                record.timestamp());
    }
}
