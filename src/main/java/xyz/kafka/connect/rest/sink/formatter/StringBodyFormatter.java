package xyz.kafka.connect.rest.sink.formatter;

import com.alibaba.fastjson2.JSON;
import xyz.kafka.connect.rest.AbstractRestConfig;

import java.util.List;
import java.util.stream.Collectors;

/**
 * StringBodyFormatter
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2023/8/12
 */
public class StringBodyFormatter implements BodyFormatter {
    private final AbstractRestConfig config;

    public StringBodyFormatter(AbstractRestConfig config) {
        this.config = config;
    }

    @Override
    public <T> String formatUpdate(List<T> records) {
        return records.stream()
                .map(JSON::toJSONString)
                .collect(Collectors.joining(
                                this.config.batchSeparator(),
                                this.config.batchPrefix(),
                                this.config.batchSuffix()
                        )
                );
    }
}