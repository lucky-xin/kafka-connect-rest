package xyz.kafka.connect.rest.sink.writer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * HttpWriter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public interface HttpWriter extends AutoCloseable {
    void put(Collection<SinkRecord> records);

    boolean failed();
}
