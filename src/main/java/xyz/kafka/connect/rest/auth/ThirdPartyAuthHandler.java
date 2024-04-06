package xyz.kafka.connect.rest.auth;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.client.HttpClientFactory;

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

    /**
     * 获取授权头部值，用于OAuth2认证。
     * 此方法会构建一个向第三方请求访问令牌的HTTP请求，然后处理第三方服务器的响应，
     * 从响应中提取出访问令牌，并构造相应的授权头返回。
     *
     * @return Header 包含授权信息的头部对象。
     * @throws IOException 如果请求执行失败，或者无法从响应中提取出有效的访问令牌。
     */
    @SuppressWarnings("all")
    @Override
    public Header getAuthorizationHeaderValue() throws IOException {
        log.debug("Configuring OAuth2 Authentication for this connection.");
        // 创建向第三方服务请求令牌的HTTP POST请求
        HttpPost req = this.createThirdPartyTokenRequest();

        // 定义处理第三方服务响应的处理器
        HttpClientResponseHandler<Header> handler = resp -> {
            try (HttpEntity entity = resp.getEntity()) {
                int status = resp.getCode();
                // 如果响应状态码不是成功或重定向，抛出异常
                if (!(status >= HttpStatus.SC_SUCCESS && status < HttpStatus.SC_REDIRECTION)) {
                    String error = resp.getReasonPhrase();
                    throw new IOException(String.format("Received a non-2xx response from the configured OAuth token URL." +
                                    " REST Response code: %d, %s, url: %s : %s", status, resp.getReasonPhrase(),
                            this.config.oauthTokenUrl(), error));
                } else {
                    // 从响应实体中提取令牌
                    String oauthResponseString = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    JSONObject node = JSON.parseObject(oauthResponseString);
                    Object eval = this.config.thirdPartyAccessTokenPointer().eval(node);
                    // 如果无法从响应中找到访问令牌，抛出异常
                    if (eval == null) {
                        throw new IOException(String.format("Could not find auth token in %s field. response fields = %s",
                                this.config.thirdPartyAccessTokenPointer().toString(), node.keySet()));
                    } else {
                        // 构造授权头部返回
                        return new BasicHeader(this.config.thirdPartyAuthorizationHeaderName(), String.format("%s %s",
                                this.config.thirdPartyAuthorizationHeaderPrefix(), eval.toString()));
                    }
                }
            }
        };

        try {
            // 使用HTTP客户端执行请求并返回授权头部
            return this.clientFactory.syncClient().execute(req, handler);
        } catch (SocketTimeoutException var8) {
            // 处理请求超时的异常
            throw new IOException("Failed to execute REST request to fetch an OAuth token as the request timed out.", var8);
        } catch (IOException var9) {
            // 处理其他IO异常
            throw new IOException("Failed to execute REST request to fetch an OAuth token", var9);
        }
    }

    HttpPost createThirdPartyTokenRequest() {
        HttpPost post = new HttpPost(URI.create(this.config.thirdPartyTokenEndpoint()));
        post.setEntity(new StringEntity(this.config.thirdPartyTokenReqBody(), ContentType.APPLICATION_JSON));
        config.headers().forEach(post::addHeader);
        return post;
    }

}
