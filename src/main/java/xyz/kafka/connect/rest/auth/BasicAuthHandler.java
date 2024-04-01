package xyz.kafka.connect.rest.auth;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.exception.RequestFailureException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * BasicAuthHandler
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
public class BasicAuthHandler extends AuthHandlerBase {
    private static final Logger log = LoggerFactory.getLogger(BasicAuthHandler.class);

    BasicAuthHandler(AbstractRestConfig config) {
        super(config);
    }

    @Override
    public Header getAuthorizationHeaderValue() {
        log.debug("Configuring Basic Authentication for this connection");
        String userpass = String.format("%s:%s", this.config.authUsername, this.config.authPassword.value());
        return new BasicHeader("Authorization",
                String.format("Basic %s",
                        new String(Base64.getEncoder().encode(userpass.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8)));
    }

    @Override
    public boolean retryAuthentication(RequestFailureException rfe) {
        return false;
    }
}
