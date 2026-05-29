package com.shan.mq.amps.ampsbasics.producer;

import com.crankuptheamps.client.HAClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shan.mq.amps.ampsbasics.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AmpsProducer {
    private final HAClient ampsClient;

    private final ObjectMapper objectMapper;
    @Value("${amps.topic}")
    private String topicName;

    public AmpsProducer(HAClient ampsClient, ObjectMapper objectMapper) {
        this.ampsClient = ampsClient;
        this.objectMapper = objectMapper;
    }

    public void publish(Order order) {
        try {
            String json = objectMapper.writeValueAsString(order);
            ampsClient.publish(topicName, json);
            log.info("Published to AMPS [" + topicName + "]: " + json);
        } catch (Exception e) {
            log.error("Publish failed: " + e.getMessage());
            throw new RuntimeException("Failed to publish order to AMPS", e);
        }
    }
}
