package com.slk.kafka.manager.service;

import com.slk.kafka.manager.dead.DeadMessageCmd;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 20:38
 * @describe
 **/
public interface DeadMessageAppServiceI {

    /**
     * 保存消息
     * @param cmd 消息
     */
    void save(DeadMessageCmd cmd);

    /**
     * 重新发送消息 手动补偿
     * @param id 消息ID
     */
    void resendMessage(String id);
}
