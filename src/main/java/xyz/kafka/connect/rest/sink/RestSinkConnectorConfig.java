package xyz.kafka.connect.rest.sink;

import org.apache.hc.core5.http.Method;
import org.apache.kafka.common.config.ConfigDef;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connector.recommenders.Recommenders;
import xyz.kafka.connector.validator.Validators;

import java.util.List;
import java.util.Map;

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
            = "REST Request Method. Valid options are ``put``, ``post`` or ``patch``.";
    private static final String REQUEST_METHOD_DISPLAY = "Request Method";

    public RestSinkConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    @Override
    public Method reqMethod() {
        return Method.valueOf(getString(REQUEST_METHOD).toUpperCase());
    }

    public static ConfigDef config() {
        return conf()
                .define(
                        REQUEST_METHOD,
                        ConfigDef.Type.STRING,
                        REQUEST_METHOD_DEFAULT,
                        Validators.oneStringOf(List.of("put", "post", "patch"), false),
                        ConfigDef.Importance.HIGH,
                        REQUEST_METHOD_DOC,
                        "sink",
                        0,
                        ConfigDef.Width.MEDIUM,
                        REQUEST_METHOD_DISPLAY,
                        Recommenders.enumValues(Method.class, Method.GET, Method.PATCH)
                );

    }
}
