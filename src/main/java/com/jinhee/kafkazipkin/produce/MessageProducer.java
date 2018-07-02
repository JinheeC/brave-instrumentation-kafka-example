package com.jinhee.kafkazipkin.produce;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.propagation.StrictCurrentTraceContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import java.util.Properties;

@Component
public class MessageProducer {
    private Long id = 1L;
    private Producer<Long, String> tracingProducer;

    public MessageProducer() {
        KafkaTracing kafkaTracing = KafkaTracing.create(Tracing.newBuilder()
                                                               .localServiceName("producer1")
                                                               .currentTraceContext(new StrictCurrentTraceContext())
                                                               .spanReporter(AsyncReporter.create(URLConnectionSender.create(
                                                                   "http://127.0.0.1:9411/api/v2/spans")))
                                                               .build());
        this.tracingProducer = kafkaTracing.producer(new KafkaProducer<>(getProducerProperties()));
    }

    public void sendMessage() {
        tracingProducer.send(new ProducerRecord<>("testTopic", id++, "This is a test message."));
    }

    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("acks", "1");
        properties.put("key.serializer", LongSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        return properties;
    }
}
