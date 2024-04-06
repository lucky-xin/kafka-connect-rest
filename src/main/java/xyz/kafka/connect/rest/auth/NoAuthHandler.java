package xyz.kafka.connect.rest.auth;

import org.apache.hc.core5.http.HttpRequest;

/**
 * NoAuthHandler
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
public class NoAuthHandler implements AuthHandler {
    NoAuthHandler() {
    }

    @Override
    public void setAuthentication(HttpRequest... requests) {
    }
}
