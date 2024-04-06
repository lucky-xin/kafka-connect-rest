package xyz.kafka.connect.rest.source;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.Versions;
import xyz.kafka.connect.rest.model.Offset;
import xyz.kafka.connect.rest.source.reader.HttpReaderImpl;
import xyz.kafka.connector.source.RateLimitSourceTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;

/**
 * HttpSourceTask
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class RestSourceTask extends RateLimitSourceTask {

    private static final Logger log = LoggerFactory.getLogger(RestSourceTask.class);

    private Offset offset;

    private HttpReaderImpl reader;

    private RestSourceConnectorConfig config;

    @Override
    public void start(Map<String, String> settings) {
        log.info("Starting RestSourceTask");
        try {
            this.config = new RestSourceConnectorConfig(settings);
            this.reader = new HttpReaderImpl(config);
            this.offset = loadOffset(config.initialOffset());
            Map<String, String> configs = new HashMap<>(settings);
            configs.put("rate.limiter.key", config.restApiUrl());
            super.start(configs);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    private Offset loadOffset(Map<String, String> initialOffset) {
        Map<String, Object> restoredOffset = ofNullable(context)
                .map(t -> t.offsetStorageReader()
                        .offset(emptyMap())
                ).orElseGet(HashMap::new);
        return Offset.of(!restoredOffset.isEmpty() ? restoredOffset : initialOffset);
    }

    @Override
    public List<SourceRecord> doPoll() {
        return this.reader.poll(offset);
    }

    @Override
    public void commitRecord(SourceRecord sr, RecordMetadata metadata) {
        config.offsetTracker().offsets(
                Map.of(
                        Map.of("topic", metadata.topic(), "partition", metadata.partition()),
                        sr.sourceOffset()
                )
        );
    }

    @Override
    public void commit() {
        offset = config.offsetTracker()
                .lowestWatermarkOffset()
                .map(Offset::of)
                .orElse(offset);
        log.debug("Offset set to {}", offset);
    }

    @Override
    public void stop() {
        // Nothing to do, no resources to release
    }

    @Override
    public String version() {
        return Versions.VERSION;
    }
}
