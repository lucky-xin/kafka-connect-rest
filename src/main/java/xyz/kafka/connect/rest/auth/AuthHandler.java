package xyz.kafka.connect.rest.auth;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpRequest;
import xyz.kafka.connect.rest.exception.RequestFailureException;

import java.io.IOException;

/**
 * 认证处理器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
public interface AuthHandler {
    void configureAuthentication(HttpRequest... reqs) throws IOException;

    boolean retryAuthentication(RequestFailureException e);

    default Header getAuthorizationHeaderValue() throws IOException {
        return null;
    }
}
