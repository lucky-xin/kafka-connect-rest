package xyz.kafka.connect.rest.source.reader;

import com.alibaba.fastjson2.JSON;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.auth.AuthHandler;
import xyz.kafka.connect.rest.auth.AuthHandlerFactory;
import xyz.kafka.connect.rest.client.HttpClientFactory;
import xyz.kafka.connect.rest.model.Offset;
import xyz.kafka.connect.rest.source.RestSourceConnectorConfig;
import xyz.kafka.connect.rest.source.parser.HttpResponseParser;
import xyz.kafka.utils.StringUtil;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HttpReaderImpl
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class HttpReaderImpl implements HttpReader {
    private static final Logger log = LoggerFactory.getLogger(HttpReaderImpl.class);
    private final RestSourceConnectorConfig config;
    private final AuthHandler authHandler;

    private final Map<String, Object> requestBody;
    private final List<BasicNameValuePair> requestParams;
    private final HttpClientFactory clientFactory;
    private final HttpResponseParser responseParser;
    private final Retry retry;

    public HttpReaderImpl(RestSourceConnectorConfig config) {
        this(config, new HttpClientFactory(config));
    }

    HttpReaderImpl(
            RestSourceConnectorConfig config,
            HttpClientFactory clientFactory) {
        this(config, clientFactory, AuthHandlerFactory.createAuthFactory(config, clientFactory));
    }

    HttpReaderImpl(
            RestSourceConnectorConfig config,
            HttpClientFactory clientFactory,
            AuthHandler authHandler) {
        this.config = config;
        this.clientFactory = clientFactory;
        this.authHandler = authHandler;
        this.requestBody = config.requestBody();
        this.requestParams = config.requestParams();
        this.responseParser = config.getHttpResponseParser();
        this.retry = Retry.of(
                "HttpReader",
                RetryConfig.custom()
                        .retryExceptions(
                                SocketException.class, SocketTimeoutException.class, ConnectTimeoutException.class
                        ).failAfterMaxAttempts(true)
                        .maxAttempts(config.maxRetries)
                        .waitDuration(Duration.ofMillis(config.retryBackoffMs))
                        .build()
        );
    }

    private void init() {
        try {
            this.authHandler.getAuthorizationHeaderValue();
        } catch (IOException e) {
            throw new ConnectException("Could not get authorization header");
        }
    }

    private HttpUriRequestBase createRequest(Offset offset) throws IOException {
        HttpUriRequestBase req = switch (config.reqMethod()) {
            case GET -> {
                List<NameValuePair> collect = offset.toMap().entrySet().stream()
                        .map(t -> new BasicNameValuePair(t.getKey(), StringUtil.toString(t.getValue())))
                        .collect(Collectors.toList());
                collect.addAll(requestParams);
                HttpGet get = new HttpGet(URI.create(config.restApiUrl));
                get.setEntity(new UrlEncodedFormEntity(collect));
                yield get;
            }
            case POST -> {
                Map<String, Object> body = new HashMap<>(offset.toMap());
                body.putAll(requestBody);
                HttpPost post = new HttpPost(URI.create(config.restApiUrl));
                post.setEntity(new StringEntity(JSON.toJSONString(body)));
                yield post;
            }
            default -> throw new ConnectException("Unsupported method " + config.reqMethod());
        };

        this.authHandler.configureAuthentication(req);
        req.setHeaders(config.headers);
        return req;
    }

    @Override
    public List<SourceRecord> poll(Offset offset) {
        return this.retry.executeSupplier(() -> doPollRecords(offset));
    }

    @SuppressWarnings("all")
    private List<SourceRecord> doPollRecords(Offset offset) {
        try {
            HttpUriRequestBase req = this.createRequest(offset);
            HttpClientResponseHandler<List<SourceRecord>> handler = responseParser::parse;
            return clientFactory.syncClient().execute(req, handler);
        } catch (IOException e) {
            throw new ConnectException("pull data failed", e);
        }
    }

}
