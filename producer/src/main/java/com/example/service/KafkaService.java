package com.example.service;

import com.example.model.Payment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    @Autowired
    private KafkaTemplate<String, Payment> kafkaTemplate;

    public String sendMessage(Payment payment) {
        kafkaTemplate.send("payment-events", "payment", payment);
        return "A payment just published from producer";
    }
}
