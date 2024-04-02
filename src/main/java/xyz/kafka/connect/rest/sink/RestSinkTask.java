package xyz.kafka.connect.rest.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.Versions;
import xyz.kafka.connect.rest.sink.writer.HttpWriter;
import xyz.kafka.connect.rest.sink.writer.HttpWriterImpl;
import xyz.kafka.connector.sink.RateLimitSinkTask;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * HttpSinkTask
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class RestSinkTask extends RateLimitSinkTask {
    private static final Logger log = LoggerFactory.getLogger(RestSinkTask.class);

    private HttpWriter writer;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting RestSinkTask");
        RestSinkConnectorConfig config = new RestSinkConnectorConfig(props);
        this.writer = new HttpWriterImpl(config, Optional.ofNullable(context)
                .map(SinkTaskContext::errantRecordReporter)
                .orElse(null));
        Map<String,String> configs = new HashMap<>(props);
        configs.put("rate.limiter.key",config.restApiUrl());
        super.start(configs);
    }

    @Override
    public void doPut(Collection<SinkRecord> records) {
        this.writer.put(records);
    }

    @Override
    public void stop() {
        log.info("Stopping RestSinkTask");
    }

    @Override
    public String version() {
        return Versions.VERSION;
    }
}
