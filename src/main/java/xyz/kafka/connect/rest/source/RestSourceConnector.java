package xyz.kafka.connect.rest.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.Versions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * HttpSourceConnector
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class RestSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(RestSourceConnector.class);

    private Map<String, String> settings;

    @Override
    public void start(Map<String, String> settings) {
        this.settings = settings;
    }

    @Override
    public void stop() {
        settings = null;
    }

    @Override
    public ConfigDef config() {
        return RestSourceConnectorConfig.config();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RestSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting source task configurations for {} workers.", maxTasks);
        return Collections.nCopies(maxTasks, this.settings);
    }

    @Override
    public String version() {
        return Versions.VERSION;
    }
}
