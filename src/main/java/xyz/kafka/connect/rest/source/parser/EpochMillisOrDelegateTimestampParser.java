package xyz.kafka.connect.rest.source.parser;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.time.Instant;
import java.util.Map;

import static java.lang.Long.parseLong;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;


/**
 * EpochMillisOrDelegateTimestampParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class EpochMillisOrDelegateTimestampParser implements TimestampParser {

    private TimestampParser delegate;

    @Override
    public void configure(Map<String, ?> settings) {
        EpochMillisOrDelegateTimestampParserConfig config = new EpochMillisOrDelegateTimestampParserConfig(settings);
        delegate = config.getDelegateParser();
    }

    @Override
    public Instant parse(String timestamp) {
        try {
            return Instant.ofEpochMilli(parseLong(timestamp));
        } catch (NumberFormatException e) {
            return delegate.parse(timestamp);
        }
    }

    /**
     * EpochMillisOrDelegateTimestampParserConfig
     *
     * @author luchaoxin
     * @version V 1.0
     * @since 2023-03-08
     */
    public static class EpochMillisOrDelegateTimestampParserConfig extends AbstractConfig {

        private static final String PARSER_DELEGATE = "rest.response.record.timestamp.parser.delegate";

        private final TimestampParser delegateParser;

        public EpochMillisOrDelegateTimestampParserConfig(Map<String, ?> originals) {
            super(config(), originals);
            delegateParser = getConfiguredInstance(PARSER_DELEGATE, TimestampParser.class);
        }

        public static ConfigDef config() {
            return new ConfigDef()
                    .define(PARSER_DELEGATE, CLASS, DateTimeFormatterTimestampParser.class, HIGH, "Timestamp Parser Delegate Class");
        }

        public TimestampParser getDelegateParser() {
            return delegateParser;
        }
    }
}
