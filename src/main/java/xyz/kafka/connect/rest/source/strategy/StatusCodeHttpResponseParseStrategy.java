package xyz.kafka.connect.rest.source.strategy;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static xyz.kafka.connect.rest.utils.ConfigUtils.parseIntegerRangedList;

/**
 * StatusCodeHttpResponsePolicy
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class StatusCodeHttpResponseParseStrategy implements HttpResponseParseStrategy {
    private static final Logger log = LoggerFactory.getLogger(StatusCodeHttpResponseParseStrategy.class);

    private Set<Integer> processCodes;

    private Set<Integer> skipCodes;

    @Override
    public void configure(Map<String, ?> settings) {
        StatusCodeHttpResponseParseStrategyConfig config = new StatusCodeHttpResponseParseStrategyConfig(settings);
        processCodes = config.getProcessCodes();
        skipCodes = config.getSkipCodes();
    }

    @Override
    public HttpResponseOutcome resolve(ClassicHttpResponse response) {
        if (processCodes.contains(response.getCode())) {
            return HttpResponseOutcome.PROCESS;
        } else if (skipCodes.contains(response.getCode())) {
            log.warn("Unexpected HttpResponse status code: {}, continuing with no records", response.getCode());
            return HttpResponseOutcome.SKIP;
        } else {
            return HttpResponseOutcome.FAIL;
        }
    }

    /**
     * StatusCodeHttpResponsePolicyConfig
     *
     * @author luchaoxin
     * @version V 1.0
     * @since 2023-03-08
     */
    public static class StatusCodeHttpResponseParseStrategyConfig extends AbstractConfig {

        private static final String PROCESS_CODES = "rest.response.strategy.codes.process";
        private static final String SKIP_CODES = "rest.response.strategy.codes.skip";

        private final Set<Integer> processCodes;

        private final Set<Integer> skipCodes;

        public StatusCodeHttpResponseParseStrategyConfig(Map<String, ?> originals) {
            super(config(), originals);
            processCodes = parseIntegerRangedList(getString(PROCESS_CODES));
            skipCodes = parseIntegerRangedList(getString(SKIP_CODES));
        }

        public static ConfigDef config() {
            return new ConfigDef()
                    .define(PROCESS_CODES, STRING, "200..299", HIGH, "Process Codes")
                    .define(SKIP_CODES, STRING, "300..399", HIGH, "Skip Codes");
        }

        public Set<Integer> getProcessCodes() {
            return processCodes;
        }

        public Set<Integer> getSkipCodes() {
            return skipCodes;
        }
    }

}
