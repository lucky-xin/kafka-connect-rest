package xyz.kafka.connect.rest.sink.formatter;

import com.alibaba.fastjson2.JSON;
import xyz.kafka.connect.rest.sink.RestSinkConnectorConfig;

import java.util.stream.Collectors;

/**
 * BodyFormatterFactory
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class BodyFormatterFactory {
    public static BodyFormatter<Object> create(RestSinkConnectorConfig config) {
        return switch (config.reqBodyFormat()) {
            case JSON -> JSON::toJSONString;
            case STRING -> records -> records.stream()
                    .map(JSON::toJSONString)
                    .collect(Collectors.joining(
                                    config.batchSeparator(),
                                    config.batchPrefix(),
                                    config.batchSuffix()
                            )
                    );
        };
    }
}
