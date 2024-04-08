package xyz.kafka.connect.rest.source.parser;

import org.apache.kafka.common.Configurable;

import java.time.Instant;
import java.util.Map;

/**
 * TimestampParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
@FunctionalInterface
public interface TimestampParser extends Configurable {

    /**
     * Parse timestamp
     *
     * @param timestamp timestamp
     * @return Instant
     */
    Instant parse(String timestamp);


    /**
     * 配置函数，用于根据给定的键值对配置对象。
     * @param map 包含配置信息的键值对映射，键和值的具体类型不限。
     */
    @Override
    default void configure(Map<String, ?> map) {

    }
}
