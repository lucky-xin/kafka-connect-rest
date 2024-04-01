package xyz.kafka.connect.rest.validator;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.enums.AuthType;
import xyz.kafka.connector.validator.ConfigValidation;
import xyz.kafka.connector.validator.ConfigValidationResult;
import xyz.kafka.connector.validator.Validators;

import java.util.Map;

/**
 * OAuth2Validator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class OAuth2Validator implements ConfigValidation.ConfigValidator {
    private static final HeadersValidator OAUTH_HEADERS_VALIDATOR = new HeadersValidator(
            AbstractRestConfig.OAUTH_CLIENT_HEADERS,
            AbstractRestConfig.OAUTH_CLIENT_HEADER_SEPARATOR
    );

    @Override
    public void ensureValid(Map<String, Object> parsedConfig, ConfigValidationResult result) {
        if (StringUtils.equals(AuthType.OAUTH2.name(), (String) parsedConfig.get(AbstractRestConfig.AUTH_TYPE))) {
            runValidator(parsedConfig, result, AbstractRestConfig.OAUTH_TOKEN_URL, Validators.validUri("http", "https"));
            runValidator(parsedConfig, result, AbstractRestConfig.OAUTH_CLIENT_ID, Validators.nonEmptyString());
            runValidator(parsedConfig, result, AbstractRestConfig.OAUTH_CLIENT_SECRET, new NonEmptyPassword());
            OAUTH_HEADERS_VALIDATOR.ensureValid(parsedConfig, result);
        }
    }

    private boolean runValidator(Map<String, Object> parsedConfig, ConfigValidationResult result, String key, ConfigDef.Validator validator) {
        try {
            validator.ensureValid(key, parsedConfig.get(key));
            return true;
        } catch (ConfigException exception) {
            result.recordError(exception.getMessage(), key);
            return false;
        }
    }

    private static class NonEmptyPassword implements ConfigDef.Validator {
        private NonEmptyPassword() {
        }

        @Override
        public void ensureValid(String name, Object o) {
            Password p = (Password) o;
            if (p == null || StringUtils.isEmpty(p.value())) {
                throw new ConfigException(name, o, "Password must be non-empty");
            }
        }

        @Override
        public String toString() {
            return "non-empty password";
        }
    }
}
