package xyz.kafka.connect.rest.auth;

import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.client.HttpClientFactory;


/**
 * AuthHandlerFactory
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class AuthHandlerFactory {
    public static AuthHandler createAuthFactory(AbstractRestConfig config, HttpClientFactory clientFactory) {
        return switch (config.authType()) {
            case BASIC -> new BasicAuthHandler(config);
            case OAUTH2 -> new OAuth2AuthHandler(clientFactory, config);
            case NONE -> req -> {
            };
            case THIRD_PARTY -> new ThirdPartyAuthHandler(clientFactory, config);
            default ->  throw new IllegalStateException("Unexpected auth type: " + config.authType());
        };
    }


}
