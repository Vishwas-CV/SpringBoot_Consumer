package com.example.demo.Consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;

@Slf4j
@Service
public class KafkaConsumerService implements Consumer<Message<GenericRecord>> {
   // The KafkaConsumerService class is a Spring service that consumes Kafka messages -> processes them and sends the processed data to an API endpoint.

    private final ObjectMapper objectMapper = new ObjectMapper();//Used for JSON processing.
    private final RestTemplate restTemplate = new RestTemplate();//Used to make REST calls.
    @Value("${api.endpoint.url}")
    private String apiEndpoint;

    @KafkaListener(topics = "dbserver1.vishwas.outbox_event", groupId = "ehUserACLConsumer")
    public void accept(Message<GenericRecord> message) {
        log.info("Inside accept method");
        GenericRecord record = message.getPayload();
        log.info("Consumed message: {}", record);
        if (record != null) {
            log.info("If record is not equal to null");
            log.info("Consumed record: {}", record);
            GenericRecord afterValue = (GenericRecord) record.get("after");

            if (afterValue != null) {
                log.info("If afterValue is not equal to null");
                log.info("Print afterValue: {}", afterValue);

                try {
                    Long id = (Long) afterValue.get("id");
                    String aggregateType = afterValue.get("aggregate_type").toString();
                    Long aggregateId = (Long) afterValue.get("aggregate_id");
                    String eventType = afterValue.get("event_type").toString();
                    String payload = afterValue.get("payload").toString();

                    log.info("ID: {}", id);
                    log.info("Aggregate Type: {}", aggregateType);
                    log.info("Aggregate ID: {}", aggregateId);
                    log.info("Event Type: {}", eventType);
                    log.info("Payload: {}", payload);
                    log.info("Before sending to Mirth");
                    sendValueInsideAfterToMirthEndpoint(afterValue);
                    log.info("After sending to Mirth");
                } catch (Exception e) {
                    log.error("Error Inside accept():", e);
                }
            }
        }
    }

    private void sendValueInsideAfterToMirthEndpoint(GenericRecord afterValue) {
        try {
            log.info("Inside sendValueInsideAfterToMirthEndpoint: {}", afterValue);

            // Convert GenericRecord to JsonNode with base64-decoding for specific fields
            // Converts the GenericRecord to JsonNode using genericRecordToJsonNode mrthod
            JsonNode convertedAfterJson = genericRecordToJsonNode(afterValue);
            log.info("After Converting To JSON: {}", convertedAfterJson);

            // Here I am Calling the API endpoint
            String response = restTemplate.postForObject(apiEndpoint, convertedAfterJson, String.class);
            log.info("Response from API: {}", response);
        } catch (Exception e) {
            log.error("Error Inside sendValueInsideAfterToMirthEndpoint():", e);
        }
    }

    private JsonNode genericRecordToJsonNode(GenericRecord afterValue) {
        ObjectNode node = objectMapper.createObjectNode();
        afterValue.getSchema().getFields().forEach(field -> {
            String fieldName = field.name();
            Object value = afterValue.get(fieldName);
            if (value instanceof GenericRecord) {
                node.set(fieldName, genericRecordToJsonNode((GenericRecord) value));
            } else if (value instanceof String && fieldName.equals("payload")) {
                // Decode the base64-encoded payload
                String decodedPayload = new String(Base64.getDecoder().decode((String) value), StandardCharsets.UTF_8);
                node.put(fieldName, decodedPayload);
            } else if (value instanceof org.apache.avro.util.Utf8) {
                // Handle Utf8 fields
                node.put(fieldName, value.toString());
            } else {
                node.putPOJO(fieldName, value);
            }
        });
        return node;
    }
}
