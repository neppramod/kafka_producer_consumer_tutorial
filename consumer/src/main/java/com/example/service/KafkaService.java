package com.example.service;

import com.example.model.Payment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    private String message;

    @KafkaListener(topics="payment-events", groupId = "mygroup1")
    public void consumeMessage(Payment payment) {
        message = payment + " got the data from kafka";
        System.out.println(message);
    }

    public String getMessage() {
        return message;
    }

}
