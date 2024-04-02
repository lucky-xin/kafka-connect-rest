package xyz.kafka.connect.rest.sink.formatter;

import xyz.kafka.connect.rest.sink.RestSinkConnectorConfig;

/**
 * BodyFormatterFactory
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class BodyFormatterFactory {
    public static BodyFormatter create(RestSinkConnectorConfig config) {
        return switch (config.reqBodyFormat()) {
            case JSON -> new JsonBodyFormatter(config);
            case STRING -> new StringBodyFormatter(config);
        };
    }
}
