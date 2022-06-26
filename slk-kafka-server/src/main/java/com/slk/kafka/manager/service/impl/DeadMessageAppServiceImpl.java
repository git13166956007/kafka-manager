package com.slk.kafka.manager.service.impl;

import com.alibaba.fastjson2.JSON;
import com.slk.kafka.client.spring.RetryDeadKafkaListenerHandler;
import com.slk.kafka.manager.convert.DeadMessageConvert;
import com.slk.kafka.manager.dead.DeadMessageCmd;
import com.slk.kafka.manager.entity.DeadMessageEntity;
import com.slk.kafka.manager.repository.DeadMessageRepository;
import com.slk.kafka.manager.service.DeadMessageAppServiceI;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 20:38
 * @describe
 **/
@Slf4j
@Service
public class DeadMessageAppServiceImpl implements DeadMessageAppServiceI {

    @Resource
    private DeadMessageRepository deadMessageRepository;

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    private final StringSerializer stringSerializer = new StringSerializer();

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void save(DeadMessageCmd cmd) {
        DeadMessageEntity entity = DeadMessageConvert.INS.cmd2Entity(cmd);
        deadMessageRepository.saveDeadMessage(entity);
    }

    /**
     * 重新发送消息 手动补偿
     *
     * @param id 消息ID
     */
    @Override
    public void resendMessage(String id) {
        //查询、并且手动补偿
        Optional<DeadMessageEntity> optional = deadMessageRepository.findById(id);
        if (optional.isPresent()){
            DeadMessageEntity deadMessageEntity = optional.get();
            String kafkaTopic = deadMessageEntity.getKafkaTopic();
            int kafkaPartition = deadMessageEntity.getKafkaPartition();
            String headerStr = deadMessageEntity.getHeader();
            Map map = JSON.to(Map.class, headerStr);
            String serverName = map.get(RetryDeadKafkaListenerHandler.SERVER_NAME).toString();
            String retryDeadHead = map.get(RetryDeadKafkaListenerHandler.CUSTOM_HEAD_NAME).toString();
            RecordHeaders recordHeader = new RecordHeaders();
            Map to = JSON.to(Map.class, retryDeadHead);
            Object o = to.get(RetryDeadKafkaListenerHandler.RESEND_TIMES);
            //初始化重试次数
            to.put(RetryDeadKafkaListenerHandler.RETRY_TIMES,0);
            if (!ObjectUtils.isEmpty(o)){
                Integer resendTimes = (Integer)to.get(RetryDeadKafkaListenerHandler.RESEND_TIMES);
                AtomicInteger atomicInteger = new AtomicInteger(resendTimes);
                int resendMax = 3;
                to.put(RetryDeadKafkaListenerHandler.RESEND_TIMES,atomicInteger.incrementAndGet());
                if (atomicInteger.get()> resendMax){
                    throw new RuntimeException("手动补偿次数已大于3次，无法再进行手动补偿，请排查异常原因！");
                }
            }else {
                //初始化手动补偿次数
                to.put(RetryDeadKafkaListenerHandler.RESEND_TIMES,1);
                to.put(RetryDeadKafkaListenerHandler.DEAD_MESSAGE_ID, deadMessageEntity.getId());
            }
            recordHeader.add(RetryDeadKafkaListenerHandler.SERVER_NAME,stringSerializer.serialize(null,serverName));
            recordHeader.add(RetryDeadKafkaListenerHandler.CUSTOM_HEAD_NAME,stringSerializer.serialize(null,JSON.toJSONString(to)));
            String key = deadMessageEntity.getMessageKey();
            String value = deadMessageEntity.getMessageValue();
            ProducerRecord<String, String> objectStringProducerRecord = new ProducerRecord<>(kafkaTopic, kafkaPartition, System.currentTimeMillis(), key, value, recordHeader);
            kafkaTemplate.send(objectStringProducerRecord);
        }
    }
}
