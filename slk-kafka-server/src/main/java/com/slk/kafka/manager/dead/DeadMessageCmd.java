package com.slk.kafka.manager.dead;

import lombok.Data;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 20:06
 * @describe
 **/
@Data
public class DeadMessageCmd {

    private String id;

    private String kafkaTopic;

    private String kafkaGroupId;

    private Integer kafkaPartition;

    private Long deadTimestamp;

    private String messageKey;

    private String messageValue;

    private String header;

}
