package xyz.kafka.connect.rest.source.reader;

import org.apache.kafka.connect.source.SourceRecord;
import xyz.kafka.connect.rest.model.Offset;

import java.util.List;

/**
 * HttpReader
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public interface HttpReader {
    /**
     * 拉取数据
     *
     * @return
     */
    List<SourceRecord> poll(Offset offset);
}
