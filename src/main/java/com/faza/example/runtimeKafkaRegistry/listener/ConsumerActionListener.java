package com.faza.example.runtimeKafkaRegistry.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.faza.example.runtimeKafkaRegistry.model.ConsumerActionRequest;
import com.faza.example.runtimeKafkaRegistry.model.Constant;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerActionListener {

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @SneakyThrows
    @KafkaListener(topics = Constant.CONSUMER_ACTION_TOPIC,
            id = "my-message-consumer-#{T(java.util.UUID).randomUUID().toString()}")
    public void consumerActionListener(ConsumerRecord<String, String> record) {
        log.info("Consumer action listener got a new record: " + record);
        ConsumerActionRequest consumerActionRequest = objectMapper.readValue(record.value(),
                ConsumerActionRequest.class);
        processAction(consumerActionRequest);
        log.info("Consumer action listener done processing record: " + record);
    }

    private void processAction(ConsumerActionRequest request) {
        String consumerId = request.getConsumerId();
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
                consumerId);
        switch (request.getConsumerAction()) {
            case ACTIVATE:
                log.info("Running a consumer with id " + consumerId);
                listenerContainer.start();
                break;
            case PAUSE:
                log.info("Pausing a consumer with id " + consumerId);
                listenerContainer.pause();
                break;
            case RESUME:
                log.info("Resuming a consumer with id " + consumerId);
                listenerContainer.resume();
                break;
            case DEACTIVATE:
                log.info("Stopping a consumer with id " + consumerId);
                listenerContainer.stop();
                break;
            default:
                log.warn("Consumer action listener do not know action: " +
                        request.getConsumerAction());
        }
    }
}
