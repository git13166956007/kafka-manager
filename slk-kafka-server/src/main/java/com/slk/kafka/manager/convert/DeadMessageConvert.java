package com.slk.kafka.manager.convert;

import com.slk.kafka.manager.dead.DeadMessageCmd;
import com.slk.kafka.manager.entity.DeadMessageEntity;
import org.mapstruct.Mapper;
import org.mapstruct.NullValueCheckStrategy;
import org.mapstruct.NullValuePropertyMappingStrategy;
import org.mapstruct.factory.Mappers;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 21:23
 * @describe
 **/
@Mapper(nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS,nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
public interface DeadMessageConvert {
    DeadMessageConvert INS = Mappers.getMapper(DeadMessageConvert.class);

    /**
     * cmd 2 entity
     * @param cmd cmd
     * @return entity
     */
    DeadMessageEntity cmd2Entity(DeadMessageCmd cmd);
}
