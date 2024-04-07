package xyz.kafka.connect.rest.auth;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpRequest;

import java.io.IOException;

/**
 * 认证处理器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
@FunctionalInterface
public interface AuthHandler {
    /**
     * 设置认证头
     *
     * @param req
     * @throws IOException
     */
    void setAuthentication(HttpRequest req) throws IOException;

    /**
     * 这个函数是一个Java接口中的默认方法，它的功能是返回一个Header对象，该对象表示授权头部值。
     * 如果该方法没有被接口的实现类重写，那么它会返回null。如果没有抛出异常，该方法会返回null。
     *
     * @return Header
     * @throws IOException
     */
    default Header getAuthorizationHeaderValue() throws IOException {
        return null;
    }
}
