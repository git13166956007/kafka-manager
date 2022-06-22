package com.slk.kafka.manager.entity;

import lombok.Data;

import javax.persistence.*;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 20:06
 * @describe
 **/
@Data
@Entity
@Table(name = "dead_message")
public class DeadMessageEntity {

    @Id
    @Column(name = "id",columnDefinition = "varchar(200)")
    private String id;

    @Column(name = "kafka_topic")
    private String kafkaTopic;

    @Column(name = "kafka_group_id")
    private String kafkaGroupId;

    @Column(name = "kafka_partition")
    private int kafkaPartition;

    @Column(name = "dead_timestamp")
    private Long deadTimestamp;

    @Column(name = "message_key")
    private String messageKey;

    @Column(name = "message_value",columnDefinition = "text")
    private String messageValue;

    @Column(name = "header",columnDefinition = "varchar(1000)")
    private String header;

    @Column(name = "resend" ,nullable = false, columnDefinition = "bit(1) default 1")
    private Boolean resend = false;

}
