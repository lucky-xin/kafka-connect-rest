package xyz.kafka.connect.rest.sink.formatter;

import com.alibaba.fastjson2.JSON;
import xyz.kafka.connect.rest.AbstractRestConfig;

import java.util.List;

/**
 * JsonBodyFormatter
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2023/8/12
 */
public class JsonBodyFormatter implements BodyFormatter {
    private final AbstractRestConfig config;

    public JsonBodyFormatter(AbstractRestConfig config) {
        this.config = config;
    }

    @Override
    public <T> String formatUpdate(List<T> records) {
        boolean useJsonArray = this.config.batchJsonAsArray || records.size() != 1;
        if (useJsonArray) {
            return config.batchPrefix + JSON.toJSONString(records) + config.batchSuffix;
        }
        return JSON.toJSONString(records.get(0));
    }
}