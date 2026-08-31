package com.codehex.notificationservice.service;

import com.codehex.event.OrderNotification;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class NotificationService {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "order-placed", groupId = "notification-service")
    public void getOrderNumber(String orderId) {
        log.info("Received order id: {}", orderId);

        // Save Notification from orderId
        // final Notification savedNotification =
        //          notificationRepository.saveAndFlush(notification);

    }

    @KafkaListener(topics = "order-placed-email", groupId = "notification-service")
    public void getOrderNotification(String message) {
        try {
            OrderNotification orderNotification = objectMapper.readValue(message, OrderNotification.class);
            log.info("Received order id: {}", orderNotification.getOrderId());

            // Send notification
            log.info("Notification Sent: {}", orderNotification.toString());
        } catch (Exception e) {
            log.error("Error deserializing OrderNotification: {}", e.getMessage(), e);
        }
    }
}
