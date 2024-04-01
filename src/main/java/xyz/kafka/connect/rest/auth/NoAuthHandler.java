package xyz.kafka.connect.rest.auth;

import org.apache.hc.core5.http.HttpRequest;
import xyz.kafka.connect.rest.exception.RequestFailureException;

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
    public void configureAuthentication(HttpRequest... requests) {
    }

    @Override
    public boolean retryAuthentication(RequestFailureException rfe) {
        return false;
    }
}
