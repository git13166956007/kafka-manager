package com.slk.kafka.manager.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 23:49
 * @describe
 **/
@Slf4j
@Component
public class KafkaConsumerListener {

    @KafkaListener(topics = {"test1","test2"})
    public void consumer(ConsumerRecord<String, String> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic){
        String value = record.value();
        log.info("kafkaListener>>>>>>topicId:"+topic+"  value:"+value);
        int a = 1/0;
        //保存;
        ack.acknowledge();
    }
}
