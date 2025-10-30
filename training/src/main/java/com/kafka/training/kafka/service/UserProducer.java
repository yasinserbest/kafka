package com.kafka.training.kafka.service;

import com.kafka.training.AppConfigs;
import com.kafka.training.entity.User;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.concurrent.CompletableFuture;

@Service
public class UserProducer {
    private static final String TOPIC = AppConfigs.topicName;
    private final KafkaTemplate<String, User> kafkaTemplate;


    public UserProducer(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<String> sendUser(@RequestBody User user) {
        CompletableFuture<SendResult<String,User>> future = kafkaTemplate.send(TOPIC,user.getId(),user);

        return future.handle((result,ex) -> {
            if(ex != null) {
                return "X failed to send user:" + ex.getMessage();
            } else  {
                RecordMetadata metadata = result.getRecordMetadata();
                return String.format(
                        "User sent to topic%s partition %d offset %d",
                        metadata.topic(), metadata.partition(), metadata.offset()
                );

            }
        });
    }
}
