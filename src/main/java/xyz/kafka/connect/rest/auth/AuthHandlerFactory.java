package xyz.kafka.connect.rest.auth;
/*
 *Copyright © 2024 chaoxin.lu
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath;
import com.github.benmanes.caffeine.cache.Cache;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.client.HttpClientFactory;
import xyz.kafka.utils.StringUtil;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static xyz.kafka.connect.rest.AbstractRestConfig.CLIENT_AUTH_MODE_HEADER;
import static xyz.kafka.connect.rest.AbstractRestConfig.CLIENT_AUTH_MODE_URL;


/**
 * AuthHandlerFactory
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class AuthHandlerFactory {
    private static final Logger log = LoggerFactory.getLogger(AuthHandlerFactory.class);
    private final HttpClientFactory clientFactory;
    private final AbstractRestConfig config;

    public AuthHandlerFactory(HttpClientFactory clientFactory, AbstractRestConfig config) {
        this.clientFactory = clientFactory;
        this.config = config;
    }

    public AuthHandler create() {
        return switch (config.authType()) {
            case BASIC -> createBasicAuthHandler();
            case OAUTH2 -> createOAuth2AuthHandler();
            case THIRD_PARTY -> createThirdPartyAuthHandler();
            case NONE -> createNoneAuthHandler();
            default ->  throw new IllegalStateException("Unexpected auth type: " + config.authType());
        };
    }

    private AuthHandler createBasicAuthHandler() {
        return new AuthHandler() {
            @Override
            public Header createAuthorizationHeader() {
                log.debug("Configuring Basic Authentication for this connection");
                String userpass = String.format("%s:%s", config.authUsername(), config.authPassword().value());
                return new BasicHeader("Authorization",
                        String.format("Basic %s",
                                new String(Base64.getEncoder()
                                        .encode(userpass.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8)));
            }

            @Override
            public Cache<String, Header> cache() {
                return config.authCache();
            }
        };
    }

    @SuppressWarnings("all")
    private AuthHandler createOAuth2AuthHandler() {
        return new AuthHandler() {
            @Override
            public Header createAuthorizationHeader() throws IOException {
                // 创建OAuth令牌请求
                return header(
                        createOAuthTokenRequest(),
                        config.oauthTokenUrl(),
                        config.oauthTokenJsonPath(),
                        config.oauthTokenTypeJsonPath(),
                        "Authorization",
                        "Bearer"
                );
            }

            @Override
            public Cache<String, Header> cache() {
                return config.authCache();
            }
        };
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
    private AuthHandler createThirdPartyAuthHandler() {
        return new AuthHandler() {
            @Override
            public Header createAuthorizationHeader() throws IOException {
                return header(
                        createThirdPartyTokenRequest(),
                        config.thirdPartyTokenEndpoint(),
                        config.thirdPartyAccessTokenJsonPath(),
                        config.thirdPartyAccessTokenTypeJsonPath(),
                        config.thirdPartyAuthorizationHeaderName(),
                        config.thirdPartyAuthorizationHeaderPrefix()
                );
            }

            @Override
            public Cache<String, Header> cache() {
                return config.authCache();
            }
        };
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
        HttpPost post = new HttpPost(URI.create(config.oauthTokenUrl()));
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
        this.config.oauthClientHeaders().forEach(post::addHeader);
        // 设置请求体类型
        post.addHeader("Content-Type", "application/x-www-form-urlencoded");
        // 创建并设置请求体实体
        UrlEncodedFormEntity ufe = new UrlEncodedFormEntity(parameters);
        post.setEntity(ufe);
        return post;
    }

    private AuthHandler createNoneAuthHandler() {
        return () -> null;
    }

    @SuppressWarnings("all")
    private Header header(HttpPost req,
                          String endpoint,
                          JSONPath accessTokenJsonPath,
                          JSONPath accessTokenTypeJsonPath,
                          String headerName,
                          String headerPrefix) throws IOException {
        log.debug("Configuring OAuth2 Authentication for this connection.");
        // POST请求
        // 定义响应的处理器
        HttpClientResponseHandler<Header> handler = resp -> {
            try (HttpEntity entity = resp.getEntity()) {
                int status = resp.getCode();
                // 如果响应状态码不是成功或重定向，抛出异常
                if (!(status >= HttpStatus.SC_SUCCESS && status < HttpStatus.SC_REDIRECTION)) {
                    String error = resp.getReasonPhrase();
                    throw new IOException(String.format("Received a non-2xx response from the configured OAuth token URL." +
                            " REST Response code: %d, %s, url: %s : %s", status, resp.getReasonPhrase(), endpoint, error));
                } else {
                    // 从响应实体中提取令牌
                    String oauthResponseString = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    JSONObject node = JSON.parseObject(oauthResponseString);
                    Object eval = accessTokenJsonPath.eval(node);
                    // 如果无法从响应中找到访问令牌，抛出异常
                    if (eval == null) {
                        throw new IOException(String.format("Could not find auth token in %s field. response fields = %s",
                                accessTokenJsonPath, node.keySet()));
                    } else {
                        String tokenType = Optional.ofNullable(accessTokenTypeJsonPath.eval(node))
                                .map(StringUtil::toString)
                                .orElse(headerPrefix);
                        // 构造授权头部返回
                        return new BasicHeader(headerName, String.format("%s %s", tokenType, eval));
                    }
                }
            }
        };

        try {
            // 使用HTTP客户端执行请求并返回授权头部
            return this.clientFactory.syncClient().execute(req, handler);
        } catch (SocketTimeoutException var8) {
            throw new IOException("Failed to execute REST request to fetch an OAuth token as the request timed out.", var8);
        } catch (IOException var9) {
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
