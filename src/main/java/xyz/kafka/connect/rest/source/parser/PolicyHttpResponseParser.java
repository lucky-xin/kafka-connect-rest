package xyz.kafka.connect.rest.source.parser;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import xyz.kafka.connect.rest.source.polic.HttpResponsePolicy;
import xyz.kafka.connect.rest.source.polic.StatusCodeHttpResponsePolicy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;

/**
 * PolicyHttpResponseParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class PolicyHttpResponseParser implements HttpResponseParser {

    private HttpResponseParser delegate;

    private HttpResponsePolicy policy;

    @Override
    public void configure(Map<String, ?> settings) {
        PolicyHttpResponseParserConfig config = new PolicyHttpResponseParserConfig(settings);
        delegate = config.getDelegateParser();
        policy = config.getPolicy();
    }

    @Override
    public List<SourceRecord> parse(ClassicHttpResponse resp) {
        return switch (policy.resolve(resp)) {
            case PROCESS -> delegate.parse(resp);
            case SKIP -> emptyList();
            default -> throw new IllegalStateException(String.format("Policy failed for response code: %s, body: %s",
                    resp.getCode(), ofNullable(resp.getEntity())
                            .map(t -> {
                                try {
                                    String result = EntityUtils.toString(t, StandardCharsets.UTF_8);
                                    EntityUtils.consume(t);
                                    return result;
                                } catch (IOException | ParseException e) {
                                    throw new IllegalStateException(e);
                                }
                            }).orElse("")));
        };
    }

    /**
     * PolicyHttpResponseParserConfig
     *
     * @author luchaoxin
     * @version V 1.0
     * @since 2023-03-08
     */
    public static class PolicyHttpResponseParserConfig extends AbstractConfig {

        private static final String PARSER_DELEGATE = "rest.response.policy.parser";
        private static final String POLICY = "rest.response.policy";

        private final HttpResponseParser delegateParser;

        private final HttpResponsePolicy policy;

        public PolicyHttpResponseParserConfig(Map<String, ?> originals) {
            super(config(), originals);
            delegateParser = getConfiguredInstance(PARSER_DELEGATE, HttpResponseParser.class);
            policy = getConfiguredInstance(POLICY, HttpResponsePolicy.class);
        }

        public static ConfigDef config() {
            return new ConfigDef()
                    .define(PARSER_DELEGATE, CLASS, FastJsonRecordParser.class, HIGH, "Response Parser Delegate Class")
                    .define(POLICY, CLASS, StatusCodeHttpResponsePolicy.class, HIGH, "Response Policy Class");
        }

        public HttpResponseParser getDelegateParser() {
            return delegateParser;
        }

        public HttpResponsePolicy getPolicy() {
            return policy;
        }
    }
}
