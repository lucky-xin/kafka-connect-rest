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

import cn.hutool.core.lang.Pair;
import com.alibaba.fastjson2.JSONPath;
import org.apache.hc.core5.http.Method;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connector.recommenders.Recommenders;
import xyz.kafka.connector.validator.FilePathValidator;
import xyz.kafka.connector.validator.Validators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * HttpSinkConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-19
 */
public class RestSinkConnectorConfig extends AbstractRestConfig {

    public static final String REQUEST_METHOD = "rest.request.method";
    public static final String REQUEST_METHOD_DEFAULT = "POST";
    private static final String REQUEST_METHOD_DOC
            = "REST Request Method. Valid options are ``PUT``, ``POST``.";
    private static final String REQUEST_METHOD_DISPLAY = "Request Method";
    private static final String RECORD_KEY_JSON_PATH = "rest.record.key.json.path";
    private static final String RECORD_KEY_JSON_PATH_DOC = "Field name and JSON path key-value pair of key. eg:id=$.id";
    private static final String RECORD_KEY_JSON_PATH_DISPLAY = RECORD_KEY_JSON_PATH_DOC;
    private static final String RECORD_KEY_JSON_PATH_DEFAULT = "id=$.id";

    public static final String REQUEST_BODY_JSON_TEMPLATE = "rest.request.body.json.template";
    private static final String REQUEST_BODY_JSON_TEMPLATE_DOC = "The absolute path of json template file";
    private static final String REQUEST_BODY_JSON_TEMPLATE_DISPLAY = "The absolute path of json template file";
    public static final String REQUEST_BODY_FORMAT = "rest.request.body.format";
    private static final String REQUEST_BODY_FORMAT_DOC = "Used to produce request body in either JSON or String format";
    private static final String REQUEST_BODY_FORMAT_DISPLAY = "Request Body Format";
    private final Method method;
    private final Optional<Pair<String, JSONPath>> keyJsonPath;
    private final Optional<String> bodyTemplate;
    private final RecordFormat reqBodyFormat;


    public RestSinkConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.keyJsonPath = Optional.of(getString(RECORD_KEY_JSON_PATH))
                .map(t -> t.split("="))
                .map(t -> Pair.of(t[0].strip(), JSONPath.of(t[1].strip())));
        this.bodyTemplate = Optional.ofNullable(getString(REQUEST_BODY_JSON_TEMPLATE))
                .map(t -> {
                    try (InputStream in = Files.newInputStream(Path.of(t))) {
                        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                });
        this.method = Method.valueOf(getString(REQUEST_METHOD).toUpperCase());
        this.reqBodyFormat = RecordFormat.valueOfIgnoreCase(getString(REQUEST_BODY_FORMAT));
    }

    public static ConfigDef config() {
        return conf()
                .define(
                        REQUEST_METHOD,
                        ConfigDef.Type.STRING,
                        REQUEST_METHOD_DEFAULT,
                        Validators.oneStringOf(List.of("POST", "PUT"), false),
                        ConfigDef.Importance.HIGH,
                        REQUEST_METHOD_DOC,
                        "sink",
                        0,
                        ConfigDef.Width.MEDIUM,
                        REQUEST_METHOD_DISPLAY,
                        Recommenders.enumValues(Method.class, Method.GET, Method.PATCH)
                ).define(
                        RECORD_KEY_JSON_PATH,
                        ConfigDef.Type.STRING,
                        RECORD_KEY_JSON_PATH_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        RECORD_KEY_JSON_PATH_DOC,
                        "sink",
                        2,
                        ConfigDef.Width.MEDIUM,
                        RECORD_KEY_JSON_PATH_DISPLAY
                ).define(
                        REQUEST_BODY_JSON_TEMPLATE,
                        ConfigDef.Type.STRING,
                        null,
                        new FilePathValidator(".json"),
                        ConfigDef.Importance.HIGH,
                        REQUEST_BODY_JSON_TEMPLATE_DOC,
                        "sink",
                        3,
                        ConfigDef.Width.SHORT,
                        REQUEST_BODY_JSON_TEMPLATE_DISPLAY
                ).define(
                        REQUEST_BODY_FORMAT,
                        ConfigDef.Type.STRING,
                        REQUEST_BODY_FORMAT_DEFAULT,
                        Validators.oneOf(RecordFormat.asStringList()),
                        ConfigDef.Importance.MEDIUM,
                        REQUEST_BODY_FORMAT_DOC,
                        "sink",
                        4,
                        ConfigDef.Width.MEDIUM,
                        REQUEST_BODY_FORMAT_DISPLAY
                );
    }

    @Override
    public Method reqMethod() {
        return method;
    }

    public Optional<Pair<String, JSONPath>> keyJsonPath() {
        return keyJsonPath;
    }

    public Optional<String> bodyTemplate() {
        return bodyTemplate;
    }

    public RecordFormat reqBodyFormat() {
        return reqBodyFormat;
    }
}
