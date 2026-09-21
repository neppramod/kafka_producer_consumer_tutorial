package com.example.controller;

import com.example.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
    @Autowired
    private KafkaService service;

    @GetMapping("/get-payment")
    public ResponseEntity<String> getPayment() {
        String response = service.getMessage();
        return new ResponseEntity<String>(response, HttpStatus.OK);
    }
}
