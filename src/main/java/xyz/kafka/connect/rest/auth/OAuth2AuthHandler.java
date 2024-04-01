package xyz.kafka.connect.rest.auth;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.connect.rest.client.HttpClientFactory;
import xyz.kafka.connect.rest.exception.RequestFailureException;
import xyz.kafka.connect.rest.utils.HeaderConfigParser;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

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
        this.headers = headerConfigParser.parseHeadersConfig("oauth2.client.headers",
                this.config.oauthClientHeaders,
                this.config.oauthClientHeaderSeparator
        );
    }

    @Override
    public boolean retryAuthentication(RequestFailureException rfe) {
        return rfe != null && rfe.statusCode() != null && rfe.statusCode() == 401;
    }

    @Override
    @SuppressWarnings("all")
    public Header getAuthorizationHeaderValue() throws IOException {
        log.debug("Configuring OAuth2 Authentication for this connection.");
        HttpPost req = this.createOAuthTokenRequest();
        HttpClientResponseHandler<BasicHeader> handler = resp -> {
            int status = resp.getCode();
            if (!(status > 199 && status < 300)) {
                String error = resp.getReasonPhrase();
                throw new IOException(String.format("Received a non-2xx response from the configured OAuth token URL. " +
                        "REST Response code: %d, %s, url: %s : %s", status, this.config.oauthTokenUrl, this.config.oauthTokenUrl, error));
            } else {
                try (HttpEntity entity = resp.getEntity()) {
                    String oauthResponseString = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    Map<String, Object> map = JSON.parseObject(oauthResponseString);
                    Object eval = this.config.oauthTokenPath.eval(map);
                    String authString = (String) eval;
                    if (authString == null) {
                        throw new IOException(String.format("Could not find auth token in %s field. response fields = %s",
                                this.config.oauthTokenPath, map.keySet()));
                    } else {
                        String tokenType = this.capitalize(map.getOrDefault("token_type", "Bearer"));
                        return new BasicHeader("Authorization", String.format("%s %s", tokenType, authString));
                    }
                }
            }
        };
        try {
            return this.clientFactory.syncClient().execute(req, handler);
        } catch (ConnectTimeoutException var8) {
            throw new IOException("Failed to execute REST request to fetch an OAuth token as the request timed out.", var8);
        } catch (IOException var9) {
            throw new IOException("Failed to execute REST request to fetch an OAuth token", var9);
        }
    }

    HttpPost createOAuthTokenRequest() throws IOException {
        HttpPost post = new HttpPost(URI.create(this.config.oauthTokenUrl));
        List<NameValuePair> parameters = new ArrayList<>(8);
        parameters.add(new BasicNameValuePair("grant_type", "password"));
        if (StringUtils.isNotBlank(this.config.oauthClientScope)) {
            parameters.add(new BasicNameValuePair("scope", this.config.oauthClientScope));
        }


        switch (this.config.oauthSecretsEncoding) {
            case CLIENT_AUTH_MODE_HEADER -> {
                log.debug("Setting credentials in request header");
                String auth = this.config.oauthClientId + ":" + this.config.oauthClientSecret.value();
                byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
                String authHeader = "Basic " + new String(encodedAuth, StandardCharsets.UTF_8);
                post.addHeader("Authorization", authHeader);
            }
            case CLIENT_AUTH_MODE_URL -> {
                log.debug("Setting credentials in url");
                parameters.add(new BasicNameValuePair("client_id", this.config.oauthClientId));
                parameters.add(new BasicNameValuePair("client_secret", this.config.oauthClientSecret.value()));
            }
            default -> throw new IOException(String.format("Invalid option for Oauth secret encoding: %s",
                    this.config.oauthSecretsEncoding));
        }
        parameters.add(new BasicNameValuePair("username", config.authUsername));
        parameters.add(new BasicNameValuePair("password", config.authPassword.value()));
        this.headers.forEach(post::addHeader);
        post.addHeader("Content-Type", "application/x-www-form-urlencoded");
        UrlEncodedFormEntity ufe = new UrlEncodedFormEntity(parameters);
        post.setEntity(ufe);
        return post;
    }

    private String capitalize(Object tokenTypeValue) {
        String tokenType = tokenTypeValue.toString();
        return tokenType.substring(0, 1).toUpperCase() + tokenType.substring(1);
    }
}
