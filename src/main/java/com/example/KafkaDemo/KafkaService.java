package com.example.KafkaDemo;

import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class KafkaService {

    private KafkaProducer<String, String> kafkaProducer;
    private Faker faker = new Faker();

    public String sendMessage() throws ExecutionException, InterruptedException {
        if (kafkaProducer == null) {
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
            properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducer = new KafkaProducer<>(properties);
        }
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", faker.name().firstName(), faker.book().title());
        RecordMetadata metadata = kafkaProducer.send(record).get();

        return String.format("message key: %s, value: %s send succeed. offset %s, topic %s, partition %s",
                record.key(),
                record.value(),
                metadata.offset(),
                metadata.topic(),
                metadata.partition());
    }
}

