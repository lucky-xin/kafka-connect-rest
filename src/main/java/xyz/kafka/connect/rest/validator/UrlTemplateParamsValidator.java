package xyz.kafka.connect.rest.validator;

import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.utils.TemplateUrlUtils;
import xyz.kafka.connector.validator.ConfigValidation;
import xyz.kafka.connector.validator.ConfigValidationResult;

import java.util.Map;

/**
 * UrlTemplateParamsValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class UrlTemplateParamsValidator implements ConfigValidation.ConfigValidator {
    @Override
    public void ensureValid(Map<String, Object> parsedConfig, ConfigValidationResult result) {
        for (String token : TemplateUrlUtils.getInterestingTemplateTokensFromUrl((String) parsedConfig.get(AbstractRestConfig.REST_API_URL))) {
            if (token.chars().filter(ch -> ch == 46).count() >= 10) {
                result.recordError(String.format("Template token: %s specified in the url has more literals than the max depth: %d", token, 10),
                        AbstractRestConfig.REST_API_URL);
                return;
            }
        }
    }
}
