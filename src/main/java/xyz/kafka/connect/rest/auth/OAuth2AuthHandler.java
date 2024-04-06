package xyz.kafka.connect.rest.auth;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.client.HttpClientFactory;
import xyz.kafka.connect.rest.utils.HeaderConfigParser;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static xyz.kafka.connect.rest.AbstractRestConfig.OAUTH_CLIENT_HEADERS;
import static xyz.kafka.connect.rest.sink.RestSinkConnectorConfig.CLIENT_AUTH_MODE_HEADER;
import static xyz.kafka.connect.rest.sink.RestSinkConnectorConfig.CLIENT_AUTH_MODE_URL;

/**
 * OAuth2AuthHandler
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
public class OAuth2AuthHandler extends AuthHandlerBase {
    private static final Logger log = LoggerFactory.getLogger(OAuth2AuthHandler.class);
    private final HttpClientFactory clientFactory;
    private final List<Header> headers;

    OAuth2AuthHandler(HttpClientFactory clientFactory, AbstractRestConfig config) {
        super(config);
        this.clientFactory = clientFactory;
        HeaderConfigParser headerConfigParser = HeaderConfigParser.getInstance();
        this.headers = headerConfigParser.parseHeadersConfig(
                OAUTH_CLIENT_HEADERS,
                this.config.oauthClientHeaders(),
                this.config.oauthClientHeaderSeparator()
        );
    }

    /**
     * 获取授权认证头信息。
     * 该方法用于通过OAuth2流程获取授权令牌，并将其作为"Authorization"头部值返回。
     *
     * @return Header 返回包含授权令牌的"Authorization"头部对象。
     * @throws IOException 如果在获取OAuth令牌过程中出现网络错误或解析错误时抛出。
     */
    @Override
    @SuppressWarnings("all")
    public Header getAuthorizationHeaderValue() throws IOException {
        log.debug("Configuring OAuth2 Authentication for this connection.");
        // 创建OAuth令牌请求
        HttpPost req = this.createOAuthTokenRequest();

        // 定义HTTP响应处理器，用于处理获取令牌的响应
        HttpClientResponseHandler<BasicHeader> handler = resp -> {
            int status = resp.getCode();
            // 检查响应状态码是否为成功
            if (!(status >= HttpStatus.SC_SUCCESS && status < HttpStatus.SC_REDIRECTION)) {
                String error = resp.getReasonPhrase();
                // 如果非2xx响应，则抛出IO异常
                throw new IOException(String.format("Received a non-2xx response from the configured OAuth token URL. " +
                        "REST Response code: %d, %s, url: %s : %s", status, this.config.oauthTokenUrl(), this.config.oauthTokenUrl(), error));
            } else {
                try (HttpEntity entity = resp.getEntity()) {
                    // 从响应实体中读取令牌响应字符串，并消费实体
                    String oauthResponseString = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    // 将响应字符串解析为Map对象
                    Map<String, Object> map = JSON.parseObject(oauthResponseString);
                    // 使用配置路径提取令牌值
                    Object eval = this.config.oauthTokenPath().eval(map);
                    String authString = (String) eval;
                    // 如果无法提取到令牌，则抛出IO异常
                    if (authString == null) {
                        throw new IOException(String.format("Could not find auth token in %s field. response fields = %s",
                                this.config.oauthTokenPath(), map.keySet()));
                    } else {
                        // 根据响应中"token_type"字段，设置令牌类型，并构造授权头部
                        String tokenType = this.capitalize(map.getOrDefault("token_type", "Bearer"));
                        return new BasicHeader("Authorization", String.format("%s %s", tokenType, authString));
                    }
                }
            }
        };
        try {
            // 使用客户端执行请求，并返回授权头部
            return this.clientFactory.syncClient().execute(req, handler);
        } catch (ConnectTimeoutException var8) {
            // 处理连接超时异常
            throw new IOException("Failed to execute REST request to fetch an OAuth token as the request timed out.", var8);
        } catch (IOException var9) {
            // 处理其他IO异常
            throw new IOException("Failed to execute REST request to fetch an OAuth token", var9);
        }
    }

    /**
     * 创建OAuth令牌请求的HttpPost对象。
     * 该方法根据配置信息，构建一个指向OAuth令牌URL的HTTP POST请求，包含必要的认证信息和请求参数。
     *
     * @return HttpPost 一个配置好的HttpPost对象，可用于发送OAuth令牌请求。
     * @throws IOException 如果配置的OAuth密钥编码模式无效，抛出此异常。
     */
    HttpPost createOAuthTokenRequest() throws IOException {
        // 初始化HTTP POST请求并设置目标URL
        HttpPost post = new HttpPost(URI.create(this.config.oauthTokenUrl()));
        // 准备请求参数
        List<NameValuePair> parameters = new ArrayList<>(8);
        parameters.add(new BasicNameValuePair("grant_type", "password"));
        // 如果有指定的OAuth客户端范围，则添加到参数列表
        if (StringUtils.isNotBlank(this.config.oauthClientScope())) {
            parameters.add(new BasicNameValuePair("scope", this.config.oauthClientScope()));
        }

        // 根据配置，将认证信息添加到请求的头部或URL中
        switch (this.config.oauthSecretsEncoding()) {
            case CLIENT_AUTH_MODE_HEADER -> {
                log.debug("Setting credentials in request header");
                String auth = this.config.oauthClientId() + ":" + this.config.oauthClientSecret().value();
                byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
                String authHeader = "Basic " + new String(encodedAuth, StandardCharsets.UTF_8);
                post.addHeader("Authorization", authHeader);
            }
            case CLIENT_AUTH_MODE_URL -> {
                log.debug("Setting credentials in url");
                parameters.add(new BasicNameValuePair("client_id", this.config.oauthClientId()));
                parameters.add(new BasicNameValuePair("client_secret", this.config.oauthClientSecret().value()));
            }
            default -> throw new IOException(String.format("Invalid option for Oauth secret encoding: %s",
                    this.config.oauthSecretsEncoding()));
        }
        // 添加认证用户名和密码到请求参数中
        parameters.add(new BasicNameValuePair("username", config.authUsername()));
        parameters.add(new BasicNameValuePair("password", config.authPassword().value()));
        // 添加额外的请求头
        this.headers.forEach(post::addHeader);
        // 设置请求体类型
        post.addHeader("Content-Type", "application/x-www-form-urlencoded");
        // 创建并设置请求体实体
        UrlEncodedFormEntity ufe = new UrlEncodedFormEntity(parameters);
        post.setEntity(ufe);
        return post;
    }


    private String capitalize(Object tokenTypeValue) {
        String tokenType = tokenTypeValue.toString();
        return tokenType.substring(0, 1).toUpperCase() + tokenType.substring(1);
    }
}
