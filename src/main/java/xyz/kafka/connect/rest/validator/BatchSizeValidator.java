package xyz.kafka.connect.rest.validator;

import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.utils.TemplateUrlUtils;
import xyz.kafka.connector.validator.ConfigValidation;
import xyz.kafka.connector.validator.ConfigValidationResult;

import java.util.Map;

/**
 * BatchSizeValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class BatchSizeValidator implements ConfigValidation.ConfigValidator {
    @Override
    public void ensureValid(Map<String, Object> parsedConfig, ConfigValidationResult result) {
        boolean batchJsonAsArray = (Boolean) parsedConfig.get(AbstractRestConfig.BATCH_JSON_AS_ARRAY);
        int batchMaxSize = (Integer) parsedConfig.get(AbstractRestConfig.BATCH_MAX_SIZE);
        if (batchMaxSize <= 1) {
            return;
        }
        if (!batchJsonAsArray) {
            result.recordError(String.format("%s should be set to 1 when disabling %s, but value is %s",
                    AbstractRestConfig.BATCH_MAX_SIZE, AbstractRestConfig.BATCH_JSON_AS_ARRAY, batchMaxSize), AbstractRestConfig.BATCH_MAX_SIZE);
        } else if (TemplateUrlUtils.doesUrlContainInterestingTemplateTokens((String) parsedConfig.get(AbstractRestConfig.REST_API_URL))) {
            result.recordError("Batch size > 1 can not be used with template URL params referring data from Kafka records.", AbstractRestConfig.BATCH_MAX_SIZE);
        }
    }
}
