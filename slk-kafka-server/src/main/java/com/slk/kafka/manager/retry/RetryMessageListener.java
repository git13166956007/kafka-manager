package com.slk.kafka.manager.retry;

import com.alibaba.fastjson2.JSON;
import com.slk.kafka.client.spring.RetryDeadKafkaListenerHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * @author Sherlock
 * @dae 2022/6/19 0019 20:02
 * @describe
 **/
@Slf4j
@Component
public class RetryMessageListener {

    @Resource
    private KafkaTemplate<String,Object> kafkaTemplate;

    private static final Duration RETRY_DELAY_DURATION = Duration.ofSeconds(10);

    @KafkaListener(topics = {RetryDeadKafkaListenerHandler.RETRY_TOPIC})
    public void retryDeal(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        Header customerHead = record.headers().lastHeader(RetryDeadKafkaListenerHandler.CUSTOM_HEAD_NAME);
        if (log.isInfoEnabled()) {
            String headInfo;
            if (customerHead != null) {
                headInfo = new String(customerHead.value(), StandardCharsets.UTF_8);
            } else {
                headInfo = "";
            }
            log.info("从重试队列中获取到数据：{},重新头信息：{}", JSON.toJSONString(record.value()), headInfo);
        }
        long timestamp = record.timestamp();
        if (System.currentTimeMillis() < timestamp + RETRY_DELAY_DURATION.toMillis()) {
            try {
                Thread.sleep((timestamp + RETRY_DELAY_DURATION.toMillis()) - System.currentTimeMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            Map<String, Object> cusData = JSON.to( Map.class,new StringDeserializer().deserialize(null, customerHead.value()));
            String topic = (String) cusData.get(RetryDeadKafkaListenerHandler.CUSTOM_TOPIC);
            Integer partition = (Integer) cusData.get(RetryDeadKafkaListenerHandler.CUSTOM_PARTITION);
            kafkaTemplate.send(new ProducerRecord(topic, partition, System.currentTimeMillis(), record.key(), record.value(), record.headers()));
            ack.acknowledge();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
