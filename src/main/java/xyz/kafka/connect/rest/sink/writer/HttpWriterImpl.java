package xyz.kafka.connect.rest.sink.writer;

import com.alibaba.fastjson2.JSON;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.Method;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.auth.AuthHandler;
import xyz.kafka.connect.rest.auth.AuthHandlerFactory;
import xyz.kafka.connect.rest.client.HttpClientFactory;
import xyz.kafka.connect.rest.enums.BehaviorOnError;
import xyz.kafka.connect.rest.enums.BehaviorOnNullValues;
import xyz.kafka.connect.rest.exception.NullRecordException;
import xyz.kafka.connect.rest.exception.ReportingFailureException;
import xyz.kafka.connect.rest.exception.RequestFailureException;
import xyz.kafka.connect.rest.sink.RestSinkConnectorConfig;
import xyz.kafka.connect.rest.sink.formatter.BodyFormatter;
import xyz.kafka.connect.rest.sink.formatter.BodyFormatterFactory;
import xyz.kafka.connect.rest.sink.formatter.FastJsonWriter;
import xyz.kafka.connector.utils.StructUtil;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HttpWriterImpl
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class HttpWriterImpl implements HttpWriter {
    private static final Logger log = LoggerFactory.getLogger(HttpWriterImpl.class);
    private final RestSinkConnectorConfig config;
    private final HttpClientFactory clientFactory;
    private final BodyFormatter bodyFormatter;
    private final AuthHandler authHandler;
    private final AtomicReference<ConnectException> error;
    private final ErrantRecordReporter reporter;
    private final ContentType contentType;

    public HttpWriterImpl(RestSinkConnectorConfig config, ErrantRecordReporter reporter) {
        this(config, new HttpClientFactory(config), reporter);
    }

    HttpWriterImpl(RestSinkConnectorConfig config,
                   HttpClientFactory clientFactory,
                   ErrantRecordReporter reporter
    ) {
        this(config, clientFactory, AuthHandlerFactory.createAuthFactory(config, clientFactory), reporter);
    }

    HttpWriterImpl(
            RestSinkConnectorConfig config,
            HttpClientFactory clientFactory,
            AuthHandler authHandler,
            ErrantRecordReporter reporter) {
        this.error = new AtomicReference<>(null);
        this.config = config;
        this.clientFactory = clientFactory;
        this.authHandler = authHandler;
        this.bodyFormatter = BodyFormatterFactory.create(config);
        this.reporter = reporter;
        this.contentType = ContentType.create(config.contentType());
        init();
    }

    private void init() {
        try {
            this.authHandler.getAuthorizationHeaderValue();
            if (this.config.bodyTemplate().isPresent()) {
                JSON.register(HashMap.class, new FastJsonWriter(this.config.bodyTemplate()));
            }
        } catch (IOException e) {
            throw new ConnectException("Could not get authorization header");
        }
    }

    public void throwIfFailed() {
        if (failed()) {
            try {
                close();
            } catch (ConnectException e) {
                // if close fails, want to still throw the original exception
                log.warn("Couldn't close elasticsearch client", e);
            }
            throw error.get();
        }
    }

    @Override
    public boolean failed() {
        return error.get() != null;
    }

    @Override
    public void close() {
        log.info("Stopping HttpWriter");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        throwIfFailed();
        int total = records.size();
        if (total == 0) {
            return;
        }
        Map<Map<String, Object>, SinkRecord> batchUpdate = new HashMap<>(total);
        Map<Object, SinkRecord> batchDelete = new HashMap<>(total);
        for (SinkRecord sr : records) {
            if (sr.key() == null) {
                throw new ConnectException("key must not be null");
            }
            if (sr.valueSchema() == null || sr.valueSchema().type() != Schema.Type.STRUCT) {
                throw new ConnectException("value schema must be struct,current schema is " + sr.valueSchema().type());
            }
            Map<String, Object> value = StructUtil.fromConnectData(sr.valueSchema(), sr.value(), this.config.jsonDataConfig());
            Object key = StructUtil.fromConnectData(sr.keySchema(), sr.key(), this.config.jsonDataConfig());
            if (sr.value() == null) {
                handleTombstone(sr)
                        .ifPresent(t -> batchDelete.put(value, sr));
            } else {
                addKeyIfNeed(value, key);
                batchUpdate.put(value, sr);
            }
        }
        sendBatch(batchDelete, Method.DELETE);
        sendBatch(batchUpdate, this.config.reqMethod());
    }

    private <T> void sendBatch(Map<T, SinkRecord> batch, Method method) {
        if (batch.isEmpty()) {
            return;
        }
        throwIfFailed();
        List<T> values = new ArrayList<>(batch.keySet());
        List<T> tmps = null;
        try {
            int times;
            int num = batch.size();
            times = num / this.config.batchSize();
            if (num % this.config.batchSize() != 0) {
                times++;
            }
            for (int i = 0; i < times; i++) {
                int fromIndex = this.config.batchSize() * i;
                int toIndex = this.config.batchSize() * (i + 1);
                tmps = values.subList(fromIndex, Math.min(toIndex, num));
                sendBatch(this.config.restApiUrl(), method, tmps);
            }
        } catch (Exception e) {
            if (tmps != null && reporter != null) {
                tmps.forEach(tmp -> reporter.report(batch.get(tmp), e));
            }
            error.compareAndSet(null, new ConnectException("Bulk request failed", e));
        }
    }

    private <T> void sendBatch(String formattedUrl, Method method, List<T> records) {
        try {
            SimpleHttpRequest req = SimpleHttpRequest.create(method, URI.create(formattedUrl));
            this.configureRequest(req);
            String payload = method == Method.DELETE
                    ? this.bodyFormatter.formatDelete(records)
                    : this.bodyFormatter.formatUpdate(records);
            req.setBody(payload.getBytes(StandardCharsets.UTF_8), contentType);
            log.debug("Submitting {} request with {} records", method, records.size());
            this.executeRequestWithBackOff(req, formattedUrl, payload, records.size());
        } catch (RequestFailureException | ReportingFailureException | IOException | IllegalArgumentException e) {
            this.handleException(e);
        }
    }

    private void configureRequest(HttpRequest requestBase) throws IOException {
        this.authHandler.configureAuthentication(requestBase);
        config.headers().forEach(requestBase::addHeader);
    }

    private Optional<Object> handleTombstone(SinkRecord sr) throws NullRecordException {
        if (this.config.behaviorOnNullValues() == BehaviorOnNullValues.FAIL) {
            String errorMsg = String.format("Found record with topic=%s, partition=%s, offset=%s and key=%s having value as NULL, " +
                    "failing the Connect task.", sr.topic(), sr.kafkaPartition(), sr.kafkaOffset(), sr.key());
            throw new NullRecordException(errorMsg);
        } else {
            if (this.config.behaviorOnNullValues() == BehaviorOnNullValues.DELETE) {
                return Optional.ofNullable(StructUtil.toConnectData(sr.keySchema(), sr.key()));
            } else if (this.config.behaviorOnNullValues() == BehaviorOnNullValues.LOG) {
                log.warn("Record with topic={}, partition={}, offset={} and key={} having value as NULL," +
                                " ignoring and processing subsequent records.",
                        sr.topic(), sr.kafkaPartition(), sr.kafkaOffset(), sr.key());
            } else {
                log.trace("Ignoring record with topic={}, partition={}, offset={} and key={} having " +
                                "value as NULL and processing subsequent records.",
                        sr.topic(), sr.kafkaPartition(), sr.kafkaOffset(), sr.key());
            }
        }
        return Optional.empty();
    }

    private void executeRequestWithBackOff(
            SimpleHttpRequest req,
            String formattedUrl,
            String payloadString,
            int batchSize) {
        RequestFailureException rfe = this.executeBatchRequest(req, formattedUrl, payloadString, batchSize);
        if (rfe == null) {
            return;
        }
        log.debug("Backing off after failing to execute REST request for {} records", batchSize, rfe);
        throw rfe;
    }

    @SuppressWarnings("all")
    private RequestFailureException executeBatchRequest(
            SimpleHttpRequest request,
            String formattedUrl,
            String payloadString,
            int batchSize) {
        try {
            FutureCallback<SimpleHttpResponse> callback = new FutureCallback<>() {
                @Override
                public void completed(SimpleHttpResponse resp) {

                }

                @Override
                public void failed(Exception ex) {
                    error.compareAndSet(null, new ConnectException("http error", ex));
                }

                @Override
                public void cancelled() {
                    log.info("Request was cancelled");
                }
            };
            Future<SimpleHttpResponse> future = this.clientFactory.asyncClient().execute(request, callback);
            SimpleHttpResponse resp = future.get(this.config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            int status = resp.getCode();
            if (!(status > 199 && status < 300)) {
                String oauthResponseString = resp.getBodyText();
                return new RequestFailureException(status, resp.getReasonPhrase(),
                        payloadString, formattedUrl, oauthResponseString, null, null);
            }
            log.trace("Response code: {}, message: {}", status, resp.getReasonPhrase());
            return null;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String errorMsg = "Exception while processing REST request for a batch of " + batchSize + " records.";
            return new RequestFailureException(null, null, payloadString, formattedUrl, null, e, errorMsg);
        }
    }

    private void handleException(Exception e) {
        if (this.config.behaviorOnError() == BehaviorOnError.FAIL) {
            error.compareAndSet(null, new ConnectException(e));
        } else {
            if (this.config.behaviorOnError() == BehaviorOnError.LOG) {
                log.error("send http error.", e);
            } else {
                log.trace("send http error.", e);
            }
        }
    }

    @SuppressWarnings("all")
    private void addKeyIfNeed(Map<String, Object> m, Object key) {
        this.config.keyJsonPath()
                .ifPresent(t -> {
                    Object kv = null;
                    if (m.get(t.getKey()) != null || (kv = t.getValue().eval(m)) == null) {
                        return;
                    }
                    m.put(t.getKey(), kv);
                });
    }

}
