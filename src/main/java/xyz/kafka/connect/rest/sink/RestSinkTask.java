package xyz.kafka.connect.rest.sink;
/*
 *            Copyright © 2024 chaoxin.lu
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
 * RestSinkTask
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
