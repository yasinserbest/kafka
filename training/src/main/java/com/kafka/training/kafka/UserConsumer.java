package com.kafka.training.kafka;
import com.kafka.training.AppConfigs;
import com.kafka.training.entity.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;



@Service
public class UserConsumer {
    @KafkaListener(topics = AppConfigs.topicName,groupId = AppConfigs.groupID)
    public void consume(ConsumerRecord<String, User> record) {
        System.out.printf("Received from topic=%s partition=%d offset=%d: %s%n",
                record.topic(), record.partition(), record.offset(), record.value());
    }
}
