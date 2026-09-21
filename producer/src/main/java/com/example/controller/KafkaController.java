package com.example.controller;

import com.example.model.Payment;
import com.example.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaService service;

    @PostMapping("/add-payment")
    public ResponseEntity<String> addPayment(@RequestBody Payment payment) {
        String response = service.sendMessage(payment);
        return new ResponseEntity<String>(response, HttpStatus.OK);
    }
}
