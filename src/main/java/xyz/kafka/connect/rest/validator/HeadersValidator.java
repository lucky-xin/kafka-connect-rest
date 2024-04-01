package xyz.kafka.connect.rest.validator;


import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.kafka.common.config.ConfigException;
import xyz.kafka.connect.rest.utils.HeaderConfigParser;
import xyz.kafka.connector.validator.ConfigValidation;
import xyz.kafka.connector.validator.ConfigValidationResult;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;


/**
 * HeadersValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class HeadersValidator implements ConfigValidation.ConfigValidator {
    private static final Set<String> FORBIDDEN_HEADERS = Set.of(
            "accept-charset", "accept-encoding", "access-control-request-headers", "access-control-request-method", "connection",
            "content-length", "cookie", "cookie2", "date", "dnt",
            "expect", "feature-policy", "host", "keep-alive", "origin",
            "referer", "te", "trailer", "trailer-encoding", "upgrade", "via");
    private static final Set<String> FORBIDDEN_HEADERS_PREFIXES = Set.of("proxy-", "sec-", "x-forwarded-");
    private final String headerConfigKey;
    private final String headerSeparatorKey;
    private final HeaderConfigParser headerConfigParser = HeaderConfigParser.getInstance();

    public HeadersValidator(String headerConfigKey, String headerSeparatorKey) {
        this.headerConfigKey = headerConfigKey;
        this.headerSeparatorKey = headerSeparatorKey;
    }

    @Override
    public void ensureValid(Map<String, Object> parsedConfig, ConfigValidationResult result) {
        String headers = (String) parsedConfig.get(this.headerConfigKey);
        if (!StringUtils.isEmpty(headers)) {
            try {
                this.headerConfigParser.parseHeadersConfig(
                                this.headerConfigKey,
                                headers,
                                (String) parsedConfig.get(this.headerSeparatorKey)
                        ).stream()
                        .map(NameValuePair::getName)
                        .forEach(this::verifyHeaderNameAllowed);
            } catch (ConfigException ex) {
                result.recordError(ex.getMessage(), this.headerConfigKey);
            }
        }
    }

    private void verifyHeaderNameAllowed(String headerName) throws ConfigException {
        String lowerCaseHeaderName = headerName.toLowerCase();
        if (!FORBIDDEN_HEADERS.contains(lowerCaseHeaderName)) {
            Stream<String> stream = FORBIDDEN_HEADERS_PREFIXES.stream();
            if (stream.noneMatch(t -> t.startsWith(headerName))) {
                return;
            }
        }
        throw new ConfigException("Header is not allowed: " + headerName);
    }
}
