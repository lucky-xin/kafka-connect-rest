package xyz.kafka.connect.rest.sink.formatter;

import com.alibaba.fastjson2.JSON;

import java.util.List;
import java.util.Map;

/**
 * BodyFormatter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public interface BodyFormatter {
    /**
     * 格式化批量更新请求体
     *
     * @param records
     * @return
     */
    <T> String formatUpdate(List<T> records);

    /**
     * 格式化批量删除请求体
     *
     * @param records
     * @return
     */
    default <T> String formatDelete(List<T> records) {
        return JSON.toJSONString(Map.of("keys", records));
    }

}
