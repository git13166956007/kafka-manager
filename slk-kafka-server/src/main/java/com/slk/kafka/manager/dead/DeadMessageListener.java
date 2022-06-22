package com.slk.kafka.manager.dead;

import com.alibaba.fastjson2.JSON;
import com.slk.kafka.client.handler.RetryDeadKafkaListenerHandler;
import com.slk.kafka.manager.service.DeadMessageAppServiceI;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 20:02
 * @describe
 **/
@Slf4j
@Component
public class DeadMessageListener {

    @Resource
    private DeadMessageAppServiceI deadMessageAppServiceI;

    private final StringDeserializer stringDeserializer = new StringDeserializer();

    @KafkaListener(topics = RetryDeadKafkaListenerHandler.DEAD_TOPIC)
    public void dealDeadMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        log.info("死信队列收取到信息：{}" , JSON.toJSONString(consumerRecord.value()));
        Header retryDeadHeader = consumerRecord.headers().lastHeader(RetryDeadKafkaListenerHandler.CUSTOM_HEAD_NAME);
        Header serverNameHeader = consumerRecord.headers().lastHeader(RetryDeadKafkaListenerHandler.SERVER_NAME);
        if (retryDeadHeader == null || retryDeadHeader.value() == null) {
            log.error("死信数据未获取到源数据信息，死信偏移量：" + consumerRecord.offset());
            return;
        }
        try {
            String retryDead = stringDeserializer.deserialize(null, retryDeadHeader.value());
            String serverName = stringDeserializer.deserialize(null, serverNameHeader.value());
            Map<String, Object> cusMap = JSON.to(Map.class,retryDead);
            Map<String, Object> headerMap = new HashMap<>();
            DeadMessageCmd cmd = new DeadMessageCmd();
            Object o = cusMap.get(RetryDeadKafkaListenerHandler.DEAD_MESSAGE_ID);
            if (!ObjectUtils.isEmpty(o)){
                cmd.setId(o.toString());
            }else {
                String id = UUID.randomUUID().toString();
                cusMap.put(RetryDeadKafkaListenerHandler.DEAD_MESSAGE_ID,id);
                cmd.setId(id);
            }
            headerMap.put(RetryDeadKafkaListenerHandler.SERVER_NAME,serverName);
            headerMap.put(RetryDeadKafkaListenerHandler.CUSTOM_HEAD_NAME,cusMap);
            cmd.setKafkaTopic((String) cusMap.get(RetryDeadKafkaListenerHandler.CUSTOM_TOPIC));
            cmd.setKafkaGroupId((String) cusMap.get(RetryDeadKafkaListenerHandler.CUSTOM_GROUP_ID));
            cmd.setKafkaPartition((Integer) cusMap.get(RetryDeadKafkaListenerHandler.CUSTOM_PARTITION));
            cmd.setDeadTimestamp(consumerRecord.timestamp());
            cmd.setMessageKey(consumerRecord.key());
            cmd.setMessageValue(consumerRecord.value());
            cmd.setHeader(JSON.toJSONString(headerMap));
            deadMessageAppServiceI.save(cmd);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("死信队列数据处理失败,消息偏移量:{}, 异常信息：{}", consumerRecord.offset(), e.getMessage());
        }
    }
}
