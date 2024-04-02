package xyz.kafka.connect.rest.source;

import cn.hutool.core.text.CharPool;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Splitter;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.kafka.common.config.ConfigDef;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.source.parser.HttpResponseParser;
import xyz.kafka.connect.rest.source.parser.StrategyHttpResponseParser;
import xyz.kafka.connector.offset.SourceAsyncOffsetTracker;
import xyz.kafka.connector.recommenders.Recommenders;
import xyz.kafka.connector.validator.Validators;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * HttpSourceConnectorConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class RestSourceConnectorConfig extends AbstractRestConfig {

    private static final String OFFSET_INITIAL = "rest.request.offset.initial";

    public static final String REQUEST_METHOD = "rest.request.method";
    public static final String REQUEST_METHOD_DEFAULT = "POST";
    private static final String REQUEST_METHOD_DOC = "REST Request Method. Valid options are ``get``, ``post`` or ``patch``.";
    private static final String REQUEST_METHOD_DISPLAY = "Request Method";

    public static final String REQUEST_BODY = "rest.request.body";
    public static final String REQUEST_BODY_DEFAULT = null;
    private static final String REQUEST_BODY_DOC = "REST Request Body.";

    public static final String REQUEST_PARAMS = "rest.request.params";
    public static final String REQUEST_PARAMS_DEFAULT = null;
    private static final String REQUEST_PARAMS_DOC = "REST Request Params.";

    public static final String RESPONSE_PARSER = "rest.response.parser";
    private static final String RESPONSE_PARSER_DOC = "REST Response Parser.";
    public static final String RESPONSE_OFFSET_FIELD = "rest.response.record.offset.field";
    public static final String RESPONSE_OFFSET_FIELD_DEFAULT = "offset";
    private static final String RESPONSE_OFFSET_FIELD_DOC = "REST Response Parser.";

    private final List<BasicNameValuePair> requestParams;

    private final Map<String, String> initialOffset;
    private final HttpResponseParser responseParser;
    private final SourceAsyncOffsetTracker offsetTracker;
    private final String offsetField;

    RestSourceConnectorConfig(Map<String, ?> originals)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        super(config(), originals);
        this.requestParams = Optional.ofNullable(getString(REQUEST_PARAMS))
                .map(t ->
                        Splitter.on(CharPool.AMP)
                                .trimResults()
                                .omitEmptyStrings()
                                .withKeyValueSeparator("=")
                                .split(t)
                                .entrySet()
                ).stream()
                .flatMap(Collection::stream)
                .map(t -> new BasicNameValuePair(t.getKey(), t.getValue()))
                .toList();
        this.initialOffset = Splitter.on(CharPool.COMMA)
                .trimResults()
                .omitEmptyStrings()
                .withKeyValueSeparator("=")
                .split(getString(OFFSET_INITIAL));
        this.offsetField = getString(RESPONSE_OFFSET_FIELD);
        this.responseParser = (HttpResponseParser) getClass(RESPONSE_PARSER)
                .getConstructor()
                .newInstance();
        this.offsetTracker = new SourceAsyncOffsetTracker(offsetField);
        this.responseParser.configure(originals, this);
    }

    public static ConfigDef config() {
        return conf()
                .define(
                        OFFSET_INITIAL,
                        STRING,
                        "",
                        HIGH,
                        "Starting offset")
                .define(
                        REQUEST_BODY,
                        STRING,
                        REQUEST_BODY_DEFAULT,
                        HIGH,
                        REQUEST_BODY_DOC
                ).define(
                        RESPONSE_OFFSET_FIELD,
                        STRING,
                        RESPONSE_OFFSET_FIELD_DEFAULT,
                        HIGH,
                        RESPONSE_OFFSET_FIELD_DOC
                ).define(
                        REQUEST_PARAMS,
                        STRING,
                        REQUEST_PARAMS_DEFAULT,
                        HIGH,
                        REQUEST_PARAMS_DOC
                ).define(
                        RESPONSE_PARSER,
                        CLASS,
                        StrategyHttpResponseParser.class.getName(),
                        HIGH,
                        RESPONSE_PARSER_DOC
                ).define(
                        REQUEST_METHOD,
                        ConfigDef.Type.STRING,
                        REQUEST_METHOD_DEFAULT,
                        Validators.oneStringOf(List.of("GET", "POST"), true),
                        ConfigDef.Importance.HIGH,
                        REQUEST_METHOD_DOC,
                        "source",
                        0,
                        ConfigDef.Width.MEDIUM,
                        REQUEST_METHOD_DISPLAY,
                        Recommenders.enumValues(
                                Method.class,
                                Method.GET,
                                Method.POST
                        )
                );
    }

    @Override
    public Method reqMethod() {
        return Method.valueOf(getString(REQUEST_METHOD).toUpperCase());
    }

    public Map<String, Object> requestBody() {
        return Optional.ofNullable(getString(REQUEST_BODY))
                .map(JSON::parseObject)
                .orElseGet(JSONObject::new);
    }

    public List<BasicNameValuePair> requestParams() {
        return requestParams;
    }

    public Map<String, String> initialOffset() {
        return initialOffset;
    }


    public HttpResponseParser getHttpResponseParser() {
        return responseParser;
    }

    public String offsetField() {
        return offsetField;
    }

    public SourceAsyncOffsetTracker offsetTracker() {
        return offsetTracker;
    }
}
