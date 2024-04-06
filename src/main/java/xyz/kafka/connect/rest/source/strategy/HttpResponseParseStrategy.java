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

    HttpResponseOutcome resolve(ClassicHttpResponse response);

    @Override
    default void configure(Map<String, ?> map) {
        // Do nothing
    }

    enum HttpResponseOutcome {
        /**
         *
         */
        PROCESS, SKIP, FAIL
    }
}
