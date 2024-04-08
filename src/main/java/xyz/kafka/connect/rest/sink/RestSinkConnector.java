package xyz.kafka.connect.rest.sink;
/*
 *Copyright © 2024 chaoxin.lu
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

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
        new RestSinkConnectorConfig(props);
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
