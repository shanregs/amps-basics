package com.shan.mq.amps.ampsbasics.consumer;


import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shan.mq.amps.ampsbasics.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service

public class AmpsConsumer {
    private final HAClient ampsClient;

    @Value("${amps.queue}")
    private String queueName;

    public AmpsConsumer(HAClient ampsClient) {
        this.ampsClient = ampsClient;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startConsuming() {
        Thread consumerThread = new Thread(this::consume, "amps-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("AMPS consumer thread started for queue: {}", queueName);
    }

    private void consume() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            log.info("Starting AMPS queue consumer: {}", queueName);
            Command command = new Command("subscribe")
                    .setTopic(queueName);
            for (Message message : ampsClient.execute(command)) {
                try {
                    String data = message.getData();
                    if (data == null || data.isBlank()) {
                        continue;
                    }
                    log.info("📩 Received: {}", data);
                    Order order = objectMapper.readValue(data, Order.class);
                    processOrder(order);
                    sendAck(message,  order);
                } catch (Exception ex) {
                    log.error("❌ Processing failed (no ACK sent)", ex);
                }
            }
        } catch (Exception ex) {
            log.error("❌ Consumer failed", ex);
        }
    }
    private void sendAck(Message message, Order order) {
        try {
            if (message == null || message.getBookmark() == null) {
                log.warn("⚠️ No bookmark, skipping ACK orderId={}", order.getOrderId());
                return;
            }

            // ✅ CORRECT AMPS 5.3.5 WAY
            message.ack();

            log.info("✅ ACK successful orderId={} bookmark={}",
                    order.getOrderId(),
                    message.getBookmark());

        } catch (Exception ex) {
            log.error("❌ ACK failed orderId={}", order.getOrderId(), ex);
        }
    }
    private void processOrder(Order order) {
        log.info("🔧 Processing orderId={}, product={}",
                order.getOrderId(),
                order.getProduct());
    }
/*    private void sendAck(String subId, String bookmark, Order order) {
        try {
            if (bookmark == null || bookmark.isBlank()) {
                log.warn("⚠️ Cannot ACK: missing bookmark for orderId={}", order.getOrderId());
                return;
            }

          *//*  Command ack = new Command("ack")
                    .setSubId(subId)
                    .setBookmark(bookmark);

            ampsClient.execute(ack).close();*//*

            log.info("✅ ACK sent for orderId={} bookmark={}", order.getOrderId(), bookmark);

        } catch (Exception ex) {
            log.error("❌ Failed to send ACK for orderId={} bookmark={}", order.getOrderId(), bookmark, ex);
        }
    }*/


}