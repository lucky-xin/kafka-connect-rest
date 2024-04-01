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

    Instant parse(String timestamp);

    @Override
    default void configure(Map<String, ?> map) {

    }
}
