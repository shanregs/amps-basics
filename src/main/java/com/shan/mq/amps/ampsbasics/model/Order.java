package com.shan.mq.amps.ampsbasics.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
/*    private String id;
    private String orderId;
    private String product;
    private int quantity;
    private double price;
    private String status;*/
private String messageId;
    private String orderId;
    private String customerId;
    private Instant createdTime;
    private String payload;
}