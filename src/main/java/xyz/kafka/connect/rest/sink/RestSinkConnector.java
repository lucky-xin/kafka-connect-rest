package xyz.kafka.connect.rest.sink;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.Versions;
import xyz.kafka.connect.rest.validator.HeadersValidator;
import xyz.kafka.connect.rest.validator.OAuth2Validator;
import xyz.kafka.connect.rest.validator.UrlTemplateParamsValidator;
import xyz.kafka.connector.validator.ConfigValidation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * RestSinkConnector
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public final class RestSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(RestSinkConnector.class);
    private Map<String, String> configProps;
    RestSinkConnectorConfig config;

    @Override
    public Class<? extends Task> taskClass() {
        return RestSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting sink task configurations for {} workers.", maxTasks);
        return Collections.nCopies(maxTasks, this.configProps);
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        this.config = new RestSinkConnectorConfig(props);
    }

    @Override
    public void stop() {
        configProps = null;
    }

    @Override
    public ConfigDef config() {
        return RestSinkConnectorConfig.config();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return new ConfigValidation(
                config(),
                connectorConfigs,
                new OAuth2Validator(),
                new HeadersValidator(AbstractRestConfig.HEADERS, AbstractRestConfig.HEADER_SEPARATOR),
                new UrlTemplateParamsValidator()
        ).validate();
    }

    @Override
    public String version() {
        return Versions.VERSION;
    }
}
