package xyz.kafka.connect.rest.source.parser;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * DateTimeFormatterTimestampParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class DateTimeFormatterTimestampParser implements TimestampParser {

    private DateTimeFormatter timestampFormatter;

    @Override
    public void configure(Map<String, ?> settings) {
        timestampFormatter = new DateTimeFormatterTimestampParserConfig(settings).getRecordTimestampFormatter();
    }

    @Override
    public Instant parse(String timestamp) {
        return OffsetDateTime.parse(timestamp, timestampFormatter).toInstant();
    }

    /**
     * DateTimeFormatterTimestampParserConfig
     *
     * @author luchaoxin
     * @version V 1.0
     * @since 2023-03-08
     */
    public static class DateTimeFormatterTimestampParserConfig extends AbstractConfig {

        private static final String ITEM_TIMESTAMP_PATTERN = "rest.response.record.timestamp.parser.pattern";
        private static final String ITEM_TIMESTAMP_ZONE = "rest.response.record.timestamp.parser.zone";

        private final DateTimeFormatter recordTimestampFormatter;

        DateTimeFormatterTimestampParserConfig(Map<String, ?> originals) {
            super(config(), originals);
            recordTimestampFormatter = DateTimeFormatter.ofPattern(getString(ITEM_TIMESTAMP_PATTERN))
                    .withZone(ZoneId.of(getString(ITEM_TIMESTAMP_ZONE)));
        }

        public static ConfigDef config() {
            return new ConfigDef()
                    .define(ITEM_TIMESTAMP_PATTERN, STRING, "yyyy-MM-dd'T'HH:mm:ss[.SSS]X", LOW, "Timestamp format pattern")
                    .define(ITEM_TIMESTAMP_ZONE, STRING, "UTC", LOW, "Timestamp ZoneId");
        }

        public DateTimeFormatter getRecordTimestampFormatter() {
            return recordTimestampFormatter;
        }
    }
}
