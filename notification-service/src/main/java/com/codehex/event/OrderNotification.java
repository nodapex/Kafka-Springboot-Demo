package com.codehex.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderNotification {
    private String orderId;
    private String userId;
    private String productName;
    private int quantity;
    private double price;
    private String orderStatus;
}