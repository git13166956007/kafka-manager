package com.slk.kafka.manager.repository.impl;

import com.slk.kafka.manager.entity.DeadMessageEntity;
import com.slk.kafka.manager.repository.DeadMessageRepository;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 21:15
 * @describe
 **/
@Repository
public class DeadMessageRepositoryImpl extends SimpleJpaRepository<DeadMessageEntity,String> implements DeadMessageRepository {

    public DeadMessageRepositoryImpl(EntityManager em) {
        super(DeadMessageEntity.class, em);
    }

    /**
     * 保存死信
     *
     * @param entity 死信
     */
    @Override
    public void saveDeadMessage(DeadMessageEntity entity) {
        this.save(entity);
    }
}
