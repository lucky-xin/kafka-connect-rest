package xyz.kafka.connect.rest.auth;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.client.HttpClientFactory;
import xyz.kafka.connect.rest.exception.RequestFailureException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;

/**
 * 第三方登录认证
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
public class ThirdPartyAuthHandler extends AuthHandlerBase {

    private static final Logger log = LoggerFactory.getLogger(ThirdPartyAuthHandler.class);
    private final HttpClientFactory clientFactory;

    ThirdPartyAuthHandler(HttpClientFactory clientFactory, AbstractRestConfig config) {
        super(config);
        this.clientFactory = clientFactory;
    }

    @Override
    public boolean retryAuthentication(RequestFailureException rfe) {
        return rfe != null && rfe.statusCode() != null && rfe.statusCode() == 401;
    }

    @SuppressWarnings("all")
    @Override
    public Header getAuthorizationHeaderValue() throws IOException {
        log.debug("Configuring OAuth2 Authentication for this connection.");
        HttpPost req = this.createThirdPartyTokenRequest();
        HttpClientResponseHandler<Header> handler = resp -> {
            try (HttpEntity entity = resp.getEntity()) {
                int status = resp.getCode();
                if (!(status > 199 && status < 300)) {
                    String error = resp.getReasonPhrase();
                    throw new IOException(String.format("Received a non-2xx response from the configured OAuth token URL." +
                                    " REST Response code: %d, %s, url: %s : %s", status, resp.getReasonPhrase(),
                            this.config.oauthTokenUrl, error));
                } else {
                    String oauthResponseString = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    JSONObject node = JSON.parseObject(oauthResponseString);
                    Object eval = this.config.thirdPartyAccessTokenPointer.eval(node);
                    if (eval == null) {
                        throw new IOException(String.format("Could not find auth token in %s field. response fields = %s",
                                this.config.thirdPartyAccessTokenPointer.toString(), node.keySet()));
                    } else {
                        return new BasicHeader(this.config.thirdPartyAuthorizationHeaderName, String.format("%s %s",
                                this.config.thirdPartyAuthorizationHeaderPrefix, eval.toString()));
                    }
                }
            }
        };

        try {
            return this.clientFactory.syncClient().execute(req, handler);
        } catch (SocketTimeoutException var8) {
            throw new IOException("Failed to execute REST request to fetch an OAuth token as the request timed out.", var8);
        } catch (IOException var9) {
            throw new IOException("Failed to execute REST request to fetch an OAuth token", var9);
        }
    }

    HttpPost createThirdPartyTokenRequest() {
        HttpPost post = new HttpPost(URI.create(this.config.thirdPartyTokenEndpoint));
        post.setEntity(new StringEntity(this.config.thirdPartyTokenReqBody, ContentType.APPLICATION_JSON));
        post.setHeaders(this.config.headers);
        return post;
    }

}
