package com.example.demo.Consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;
@Slf4j
@Configuration
public class KafkaConsumerConfig {
    @Bean
    //This method returns a Consumer that processes messages containing GenericRecord objects.
    public Consumer<Message<GenericRecord>> ehUserEventConsumer() {
        log.info("Inside ehUserEventConsumer");
        return new KafkaConsumerService();
    }
}
