package xyz.kafka.connect.rest.source.parser;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import xyz.kafka.connect.rest.source.strategy.HttpResponseParseStrategy;
import xyz.kafka.connect.rest.source.strategy.StatusCodeHttpResponseParseStrategy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;

/**
 * StrategyHttpResponseParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class StrategyHttpResponseParser implements HttpResponseParser {
    public static final String PARSER_DELEGATE = "rest.response.parser.delegate";
    public static final String STRATEGY = "rest.response.parser.strategy";
    private HttpResponseParser delegate;
    private HttpResponseParseStrategy strategy;

    public static ConfigDef config() {
        return new ConfigDef()
                .define(
                        PARSER_DELEGATE,
                        CLASS,
                        FastJsonRecordParser.class.getName(),
                        HIGH,
                        "Response Parser Delegate Class"
                ).define(
                        STRATEGY,
                        CLASS,
                        StatusCodeHttpResponseParseStrategy.class.getName(),
                        HIGH,
                        "Response Policy Class"
                );
    }

    @Override
    public void configure(Map<String, ?> settings, AbstractConfig c) {
        SimpleConfig config = new SimpleConfig(config(),settings);
        delegate = config.getConfiguredInstance(PARSER_DELEGATE, HttpResponseParser.class);
        strategy = config.getConfiguredInstance(STRATEGY, HttpResponseParseStrategy.class);
        delegate.configure(settings, c);
    }

    @Override
    public List<SourceRecord> parse(ClassicHttpResponse resp) {
        return switch (strategy.resolve(resp)) {
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
}
