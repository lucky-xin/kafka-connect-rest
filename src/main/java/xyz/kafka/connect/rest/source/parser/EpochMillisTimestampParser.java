package xyz.kafka.connect.rest.source.parser;

import java.time.Instant;


/**
 * EpochMillisTimestampParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class EpochMillisTimestampParser implements TimestampParser {

    @Override
    public Instant parse(String timestamp) {
        return Instant.ofEpochMilli(Long.parseLong(timestamp));
    }
}
