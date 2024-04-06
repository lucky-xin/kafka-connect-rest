package xyz.kafka.connect.rest.auth;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpRequest;
import xyz.kafka.connect.rest.AbstractRestConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * AuthHandlerBase
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
public abstract class AuthHandlerBase implements AuthHandler {
    final AbstractRestConfig config;

    protected final Cache<String, Header> cache;

    protected AuthHandlerBase(AbstractRestConfig config) {
        this.config = config;
        cache = Caffeine.newBuilder()
                .expireAfterWrite(config.authExpiresInSeconds(), TimeUnit.SECONDS)
                .maximumSize(2)
                .ticker(Ticker.systemTicker())
                .softValues()
                .build();
    }

    @Override
    public void setAuthentication(HttpRequest... requests) {
        Header header = cache.get(requests[0].getRequestUri(), k -> {
            try {
                return this.getAuthorizationHeaderValue();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        if (header != null) {
            for (HttpRequest request : requests) {
                request.addHeader(header);
            }
        }
    }

}
