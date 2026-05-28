package com.shan.mq.amps.ampsbasics.controller;


import com.shan.mq.amps.ampsbasics.model.Order;
import com.shan.mq.amps.ampsbasics.producer.AmpsProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final AmpsProducer producer;

    public OrderController(AmpsProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody Order order) {
        log.info("Publish to Q - {}", order);
        producer.publish(order);
        return ResponseEntity.ok("Order [" + order.getOrderId() + "] queued successfully");
    }
}