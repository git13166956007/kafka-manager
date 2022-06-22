package com.slk.kafka.manager.controller;

import com.slk.kafka.manager.service.DeadMessageAppServiceI;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 20:39
 * @describe 通过调用kafka api 查询死信队列数据、做手动补发、kafka dashboard
 **/
@RestController
@RequestMapping("/kafkaManager")
public class KafkaMessageController {

    @Resource
    private KafkaTemplate<String,String> kafkaTemplate;

    @Resource
    private DeadMessageAppServiceI deadMessageAppServiceI;

    @GetMapping("/test")
    public void test(){
        System.out.println(111);
        kafkaTemplate.send(new ProducerRecord<>("test1","1","send test dead queue or retry queue"));
    }

    /**
     * 手动补偿
     * @param id 消息ID
     */
    @PostMapping("/resendMessage/{id}")
    public void resendMessage(@PathVariable("id")String id){
        deadMessageAppServiceI.resendMessage(id);
    }
}
