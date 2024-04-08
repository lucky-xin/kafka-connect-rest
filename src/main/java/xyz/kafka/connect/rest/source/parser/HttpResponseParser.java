package xyz.kafka.connect.rest.source.parser;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

/**
 * HttpResponseParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
@FunctionalInterface
public interface HttpResponseParser {

    /**
     * 解析响应结果
     *
     * @param resp 响应结果
     * @return List<SourceRecord>
     */
    List<SourceRecord> parse(ClassicHttpResponse resp);

    /**
     * 配置函数，用于根据给定的映射配置抽象配置对象。
     *
     * @param map 一个包含配置信息的键值对映射，键和值的类型可以是任意类型。
     * @param config 要进行配置的抽象配置对象，此对象通过此方法接收配置信息。
     */
    default void configure(Map<String, ?> map, AbstractConfig config) {

    }
}
