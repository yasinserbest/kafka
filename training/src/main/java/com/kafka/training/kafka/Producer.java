package com.kafka.training.kafka;

import com.kafka.training.AppConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // 3️⃣ Create a record to send
        String key = "key3";
        String value = "message3!";

        ProducerRecord<String, String> record = new ProducerRecord<>(AppConfigs.topicName, key, value);

        // 4️⃣ Send message asynchronously with callback
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.printf("✅ Sent message to topic=%s partition=%d offset=%d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            }
        });

        // 5️⃣ Always close producer
        producer.flush();
        producer.close();
    }

}
