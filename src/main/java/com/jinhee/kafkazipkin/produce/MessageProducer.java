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
import zipkin2.Span;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;

@Component
public class MessageProducer {
    private Long id = 1L;
    private Producer<Long, String> tracingProducer;

    public MessageProducer() {
        ConcurrentLinkedDeque<Span> spans = new ConcurrentLinkedDeque<>();
        Tracing tracing = Tracing.newBuilder()
                                 .currentTraceContext(new StrictCurrentTraceContext())
                                 .spanReporter(spans::add)
                                 .build();

        KafkaTracing kafkaTracing = KafkaTracing.newBuilder(tracing)
                                        .remoteServiceName("producer1")
                                        .build();
        this.tracingProducer = kafkaTracing.producer(new KafkaProducer<>(getProducerProperties()));
    }

    public void sendMessage() {
        tracingProducer.send(new ProducerRecord<>("testTopic", id++, "테스트 메세지 입니다."));
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
