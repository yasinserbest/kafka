package com.kafka.training.controller;

import com.kafka.training.AppConfigs;
import com.kafka.training.entity.User;
import com.kafka.training.kafka.service.UserProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/users")
public class UserController {
    private final UserProducer userProducer;

    public UserController(UserProducer userProducer) {
        this.userProducer = userProducer;
    }


    @PostMapping
    public CompletableFuture<String> sendUser(@RequestBody User user) {
        return userProducer.sendUser(user);
    }

}
