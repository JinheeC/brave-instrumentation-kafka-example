package com.jinhee.kafkazipkin.consumer;

import brave.Tracer;
import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.propagation.StrictCurrentTraceContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import zipkin2.Span;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;

public class FirstMessageConsumer {
    private static final Integer TIMEOUT = 500;
    private Consumer<Long, String> tracingConsumer;
    private KafkaTracing kafkaTracing;
    private Tracing tracing;

    public FirstMessageConsumer() {
        ConcurrentLinkedDeque<Span> spans = new ConcurrentLinkedDeque<>();
        this.tracing = Tracing.newBuilder()
                                 .currentTraceContext(new StrictCurrentTraceContext())
                                 .spanReporter(spans::add)
                                 .build();
        this.kafkaTracing = KafkaTracing.newBuilder(tracing)
                                        .remoteServiceName("consumer1")
                                        .build();
        this.tracingConsumer = kafkaTracing.consumer(new KafkaConsumer<>(getConsumerProperties()));
        tracingConsumer.subscribe(Collections.singleton("testTopic"));
    }

    public void consume() {
        ConsumerRecords<Long, String> records = tracingConsumer.poll(TIMEOUT);
//        for (ConsumerRecord<Long, String> record : records) {
//            Span span = kafkaTracing.nextSpan(record).name("consumeFromConsumer1").start();
//            System.out.println("컨슘 "+ record.headers());
//        }
        records.forEach(record -> process(record));
        tracingConsumer.commitSync();
    }

    private void process(ConsumerRecord<Long, String> record) {
        System.out.println("컨슘 "+ record.headers());
        brave.Span span = kafkaTracing.nextSpan(record).name("consumeFromConsumer1").start();

        try (Tracer.SpanInScope ws = tracing.tracer().withSpanInScope(span)) { // so logging can see trace ID
            doProcess(record); // do the actual work
        } catch (RuntimeException | Error e) {
            span.error(e); // make sure any error gets into the span before it is finished
            throw e;
        } finally {
            span.finish(); // ensure the span representing this processing completes.
        }
    }

    private void doProcess(ConsumerRecord<Long, String> record) {
        System.out.println("컨슘 "+ record.headers());
    }

    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "zipkin-consumer1");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", LongDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        return properties;
    }
}
