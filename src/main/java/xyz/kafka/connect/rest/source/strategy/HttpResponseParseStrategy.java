package xyz.kafka.connect.rest.source.strategy;


import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.kafka.common.Configurable;

import java.util.Map;

/**
 * HttpResponseParseStrategy
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
@FunctionalInterface
public interface HttpResponseParseStrategy extends Configurable {
    /**
     * 解析HttpResponse
     *
     * @param response HttpResponse
     * @return HttpResponseOutcome
     */
    HttpResponseOutcome resolve(ClassicHttpResponse response);

    /**
     * 配置方法。
     * 该方法用于根据给定的映射配置对象，但此实现为空，不执行任何操作。
     *
     * @param map 一个包含配置信息的映射，键和值的类型可以是任意类型。
     */
    @Override
    default void configure(Map<String, ?> map) {
        // 该方法默认不执行任何操作
    }

    enum HttpResponseOutcome {
        /**
         *
         */
        PROCESS, SKIP, FAIL
    }
}
