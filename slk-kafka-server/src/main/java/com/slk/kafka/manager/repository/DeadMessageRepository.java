package com.slk.kafka.manager.repository;

import com.slk.kafka.manager.entity.DeadMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 21:10
 * @describe
 **/
public interface DeadMessageRepository extends JpaRepository<DeadMessageEntity,String> {

    /**
     * 保存死信
     * @param entity 死信
     */
    void saveDeadMessage(DeadMessageEntity entity);
}
