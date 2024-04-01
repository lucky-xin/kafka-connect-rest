package xyz.kafka.connect.rest.source.polic;


import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.kafka.common.Configurable;

import java.util.Map;

/**
 * HttpResponsePolicy
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
@FunctionalInterface
public interface HttpResponsePolicy extends Configurable {

    HttpResponseOutcome resolve(ClassicHttpResponse response);

    @Override
    default void configure(Map<String, ?> map) {
        // Do nothing
    }

    enum HttpResponseOutcome {
        PROCESS, SKIP, FAIL
    }
}
