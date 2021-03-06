package com.jinhee.kafkazipkin.consumer;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.propagation.StrictCurrentTraceContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import java.util.Collections;
import java.util.Properties;

public class MessageConsumer {
    private static final Integer TIMEOUT = 500;
    private  String name;
    private Consumer<Long, String> tracingConsumer;
    private KafkaTracing kafkaTracing;

    public MessageConsumer(String name) {
        this.name = name;
        this.kafkaTracing = KafkaTracing.create(Tracing.newBuilder()
                                                       .localServiceName(name)
                                                       .currentTraceContext(new StrictCurrentTraceContext())
                                                       .spanReporter(AsyncReporter.create(URLConnectionSender.create(
                                                           "http://127.0.0.1:9411/api/v2/spans")))
                                                       .build());
        this.tracingConsumer = kafkaTracing.consumer(new KafkaConsumer<>(getConsumerProperties()));
        tracingConsumer.subscribe(Collections.singleton("testTopic"));
    }

    public void consume() {
        ConsumerRecords<Long, String> records = tracingConsumer.poll(TIMEOUT);
        records.forEach(this::process);
        tracingConsumer.commitSync();
    }

    private void process(ConsumerRecord<Long, String> record) {
        brave.Span span = kafkaTracing.nextSpan(record).name("doSomething").start();
        doSomething(record);
        span.finish();
    }

    private void doSomething(ConsumerRecord<Long, String> record) {
        System.out.println("consume and process a message " + record);
    }

    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "zipkin-" + name);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", LongDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        return properties;
    }
}
