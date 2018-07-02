package com.jinhee.kafkazipkin.controller;

import com.jinhee.kafkazipkin.produce.MessageProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TriggeringController {

    @Autowired
    MessageProducer messageProducer;

    @GetMapping("/produce/{numOfMessage}")
    public void produce(@PathVariable Integer numOfMessage) {
        for (int i = 0; i < numOfMessage; i++) {
            messageProducer.sendMessage();
        }
    }
}
