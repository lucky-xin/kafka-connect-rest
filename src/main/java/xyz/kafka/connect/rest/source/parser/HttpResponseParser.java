package xyz.kafka.connect.rest.source.parser;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.kafka.common.Configurable;
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
public interface HttpResponseParser extends Configurable {

    List<SourceRecord> parse(ClassicHttpResponse resp);

    @Override
    default void configure(Map<String, ?> configs) {

    }

    default void configure(Map<String, ?> map, AbstractConfig config) {

    }
}
