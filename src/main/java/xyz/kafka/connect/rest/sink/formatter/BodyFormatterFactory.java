package xyz.kafka.connect.rest.sink.formatter;
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

import com.alibaba.fastjson2.JSON;
import xyz.kafka.connect.rest.sink.RestSinkConnectorConfig;

import java.util.stream.Collectors;

/**
 * BodyFormatterFactory
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class BodyFormatterFactory {
    public static BodyFormatter<Object> create(RestSinkConnectorConfig config) {
        return switch (config.reqBodyFormat()) {
            case JSON -> JSON::toJSONString;
            case STRING -> records -> records.stream()
                    .map(JSON::toJSONString)
                    .collect(Collectors.joining(
                                    config.batchSeparator(),
                                    config.batchPrefix(),
                                    config.batchSuffix()
                            )
                    );
        };
    }
}
