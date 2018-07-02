package com.jinhee.kafkazipkin;

import com.jinhee.kafkazipkin.consumer.FirstMessageConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaZipkinApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaZipkinApplication.class, args);
        FirstMessageConsumer firstMessageConsumer = new FirstMessageConsumer();

        while (true) {
            firstMessageConsumer.consume();
        }
    }
}
