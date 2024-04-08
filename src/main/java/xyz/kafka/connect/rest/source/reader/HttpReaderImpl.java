package xyz.kafka.connect.rest.source.reader;
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
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import xyz.kafka.connect.rest.auth.AuthHandler;
import xyz.kafka.connect.rest.auth.AuthHandlerFactory;
import xyz.kafka.connect.rest.client.HttpClientFactory;
import xyz.kafka.connect.rest.model.Offset;
import xyz.kafka.connect.rest.source.RestSourceConnectorConfig;
import xyz.kafka.connect.rest.source.parser.HttpResponseParser;
import xyz.kafka.utils.StringUtil;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * HttpReaderImpl
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class HttpReaderImpl implements HttpReader {
    private final RestSourceConnectorConfig config;
    private final AuthHandler authHandler;
    private final Map<String, Object> requestBody;
    private final List<BasicNameValuePair> requestParams;
    private final HttpClientFactory clientFactory;
    private final HttpResponseParser responseParser;
    private final boolean hasContentType;

    public HttpReaderImpl(RestSourceConnectorConfig config) {
        this(config, new HttpClientFactory(config));
    }

    HttpReaderImpl(
            RestSourceConnectorConfig config,
            HttpClientFactory clientFactory) {
        this(config, clientFactory, new AuthHandlerFactory(clientFactory, config).create());
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
        this.hasContentType = this.config.headers()
                .stream()
                .anyMatch(h -> h.getName().equals(HttpHeaders.CONTENT_TYPE));
    }

    private HttpUriRequestBase createRequest(Offset offset) throws IOException {
        HttpUriRequestBase req = switch (config.reqMethod()) {
            case GET -> {
                HttpGet get = new HttpGet(URI.create(config.restApiUrl()));
                get.setEntity(new UrlEncodedFormEntity(createNameValuePairs(offset)));
                yield get;
            }
            case POST -> {
                Map<String, Object> body = new HashMap<>(offset.toMap());
                Optional.ofNullable(requestBody)
                        .ifPresent(body::putAll);
                HttpPost post = new HttpPost(URI.create(config.restApiUrl()));
                if (ContentType.APPLICATION_JSON.getMimeType().equals(config.contentType())) {
                    post.setEntity(new StringEntity(JSON.toJSONString(body), ContentType.APPLICATION_JSON));
                } else {
                    post.setEntity(new UrlEncodedFormEntity(createNameValuePairs(offset)));
                }
                yield post;
            }
            default -> throw new ConnectException("Unsupported method " + config.reqMethod());
        };
        this.authHandler.setAuthentication(req);
        config.headers().forEach(req::addHeader);
        if (!this.hasContentType) {
            req.addHeader(HttpHeaders.CONTENT_TYPE, config.contentType());
        }
        return req;
    }

    @Override
    public List<SourceRecord> poll(Offset offset) {
        return doPollRecords(offset);
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

    @NotNull
    private List<NameValuePair> createNameValuePairs(Offset offset) {
        List<NameValuePair> collect = offset.toMap().entrySet().stream()
                .map(t -> new BasicNameValuePair(t.getKey(), StringUtil.toString(t.getValue())))
                .collect(Collectors.toList());
        Optional.ofNullable(requestParams)
                .ifPresent(collect::addAll);
        return collect;
    }
}
