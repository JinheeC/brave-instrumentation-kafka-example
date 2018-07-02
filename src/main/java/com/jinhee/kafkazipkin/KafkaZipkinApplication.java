package com.jinhee.kafkazipkin;

import com.jinhee.kafkazipkin.consumer.MessageConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaZipkinApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaZipkinApplication.class, args);
        MessageConsumer messageConsumer = new MessageConsumer();

        while (true) {
            messageConsumer.consume();
        }
    }
}
