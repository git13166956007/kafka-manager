package com.slk.kafka.client.spring;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 18:59
 * @describe kafka 重试队列、死信队列实现
 **/
@Slf4j
@Component
public class RetryDeadKafkaListenerHandler implements ConsumerAwareErrorHandler, InitializingBean, EnvironmentAware {

    /**
     * 重试死信队列header
     */
    public static final String CUSTOM_HEAD_NAME = "_retry_dead_head_";
    /**
     * 手动补偿次数
     */
    public static final String RESEND_TIMES = "_resend_times_";
    /**
     * 最大重试次数
     */
    private static final Integer MAX_RETRY = 3;
    /**
     * 重试次数
     */
    public static final String RETRY_TIMES = "_retry_times_";
    /**
     * 死信队列数据库表id
     */
    public static final String DEAD_MESSAGE_ID = "_dead_message_id_";
    /**
     * 自定义topic
     */
    public static final String CUSTOM_TOPIC = "_ori_topic_";
    /**
     * 自定义group_id
     */
    public static final String CUSTOM_GROUP_ID = "_ori_group_id_";
    /**
     * 自定义partition
     */
    public static final String CUSTOM_PARTITION = "_ori_partition_";

    /**
     * 重试队列topic
     */
    public static final String RETRY_TOPIC = "_retry_topic_";

    /**
     * 死信队列topic
     */
    public static final String DEAD_TOPIC = "_dead_topic_";
    /**
     * 当前client所在 服务名称
     */
    public static final String SERVER_NAME = "_server_name_";
    /**
     * 字符串序列化
     */
    private final StringSerializer stringSerializer = new StringSerializer();
    /**
     * 反序列化
     */
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    /**
     * 服务名称
     */
    private String applicationName ;
    /**
     * 环境
     */
    private Environment environment;

    @Autowired
    private KafkaTemplate<Object,Object> kafkaTemplate;

    @Override
    public void afterPropertiesSet() throws Exception {
        String serverName = environment.getProperty("spring.application.name");
        if (StringUtils.isEmpty(serverName)){
            throw new RuntimeException("请设置applicationName或者配置spring.application.name");
        }
        log.warn("当前服务：{} 的spring-kafka 重试队列、死信队列正在配置....",serverName);
        this.applicationName = serverName;
    }

    @Override
    public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord, Consumer<?, ?> consumer) {
        log.warn("服务：{} 消费消息异常，即将进入重试队列或死信队列...",applicationName);
        //获取当前消息是不是之前的异常消息
        Header header = consumerRecord.headers().lastHeader(CUSTOM_HEAD_NAME);
        Map<String, Object> cusData = new HashMap<>(16);
        AtomicInteger retryTime = new AtomicInteger(0);
        if (!ObjectUtils.isEmpty(header)){
            String deserialize = stringDeserializer.deserialize(null, header.value());
            cusData.putAll(JSON.to(Map.class, deserialize));
            //获取重试次数
            retryTime.set((Integer) cusData.get(RETRY_TIMES));
        }
        Headers headers = consumerRecord.headers();
        String topic = consumerRecord.topic();
        int partition = consumerRecord.partition();
        AtomicLong atomicOffSet = new AtomicLong(consumerRecord.offset());
        String groupId = consumer.groupMetadata().groupId();

        //填充header信息
        this.addOriHeaderInfo(cusData,topic,partition,groupId);
        this.addAppNameHeader(headers);
        if (retryTime.get()>=MAX_RETRY){
            //进入死信队列
            dealDeadMessage(consumerRecord,headers,cusData);
        }else {
            //进入重试队列
            dealRetryMessage(consumerRecord,headers,cusData,retryTime);
        }
        consumer.commitSync(Collections.singletonMap(new TopicPartition(topic,partition),new OffsetAndMetadata(atomicOffSet.incrementAndGet())));
    }

    private void dealDeadMessage(ConsumerRecord<?, ?> consumerRecord, Headers headers, Map<String, Object> cusData) {
        log.warn("正在进入死信队列......");
        try {
            kafkaTemplate.send(new ProducerRecord<>(DEAD_TOPIC, consumerRecord.partition(),System.currentTimeMillis(), consumerRecord.key(),consumerRecord.value(),buildHeader(headers,cusData))).get();
        }catch (Exception e){
            log.error("发送死信队列异常:{}",e.getMessage());
            throw new RuntimeException("发送到死信队列异常", e);
        }
    }

    private void dealRetryMessage(ConsumerRecord<?, ?> consumerRecord, Headers headers, Map<String, Object> cusData, AtomicInteger retryTime) {
        cusData.put(RETRY_TIMES,retryTime.incrementAndGet());
        log.warn("正在第{}次进入重试队列......",retryTime.get());
        try {
            kafkaTemplate.send(new ProducerRecord<>(RETRY_TOPIC, consumerRecord.partition(),System.currentTimeMillis(), consumerRecord.key(),consumerRecord.value(),buildHeader(headers,cusData))).get();
        }catch (Exception e){
            log.error("发送重试队列异常:{}",e.getMessage());
            throw new RuntimeException("发送到重试队列异常", e);
        }
    }

    private RecordHeaders buildHeader(Headers headers, Map<String, Object> cusData) {
        RecordHeaders headersNew = new RecordHeaders(headers.toArray());
        headersNew.remove(CUSTOM_HEAD_NAME);
        headersNew.add(CUSTOM_HEAD_NAME, stringSerializer.serialize(null, JSON.toJSONString(cusData)));
        return headersNew;
    }

    private void addAppNameHeader(Headers header) {
        if (!StringUtils.isEmpty(applicationName)){
            header.remove(SERVER_NAME);
            header.add(SERVER_NAME,stringSerializer.serialize(null,applicationName));
        }
    }

    private void addOriHeaderInfo(Map<String, Object> cusData, String topic, int partition, String groupId) {
        if (cusData.size()==0){
            cusData.put(RETRY_TIMES, 0);
        }
        cusData.putIfAbsent(CUSTOM_GROUP_ID,groupId);
        cusData.putIfAbsent(CUSTOM_TOPIC,topic);
        cusData.putIfAbsent(CUSTOM_PARTITION,partition);
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
