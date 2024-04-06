package xyz.kafka.connect.rest.client;

import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.nio.AsyncClientConnectionManager;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.NoHttpResponseException;
import org.apache.hc.core5.http.URIScheme;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connect.rest.AbstractRestConfig;
import xyz.kafka.utils.StringUtil;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * HttpClientFactory
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class HttpClientFactory {
    private static final Logger log = LoggerFactory.getLogger(HttpClientFactory.class);
    private final AbstractRestConfig config;
    private final RequestConfig requestConfig;
    private final Http5RequestRetryStrategy requestRetryStrategy;
    private final AsyncClientConnectionManager asyncClientConnectionManager;

    public HttpClientFactory(AbstractRestConfig config) {
        this.config = config;
        this.requestConfig = createRequestConfig();
        this.requestRetryStrategy = requestRetryStrategy(config);
        this.asyncClientConnectionManager = createAsyncConnectionManager();
    }

    Http5RequestRetryStrategy requestRetryStrategy(AbstractRestConfig config) {
        return new Http5RequestRetryStrategy(
                config.maxRetries(),
                TimeValue.ofMilliseconds(config.retryBackoffMs()),
                List.of(SocketException.class, SocketTimeoutException.class,
                        ConnectTimeoutException.class, NoHttpResponseException.class),
                config.retryCodes()
        );
    }

    public CloseableHttpAsyncClient asyncClient() {
        HttpAsyncClientBuilder builder = HttpAsyncClients.custom();
        credentialsProvider().ifPresent(builder::setDefaultCredentialsProvider);
        CloseableHttpAsyncClient cli = builder.setConnectionManager(asyncClientConnectionManager)
                .setRetryStrategy(requestRetryStrategy)
                .setDefaultRequestConfig(requestConfig)
                .build();
        cli.start();
        return cli;
    }

    private AsyncClientConnectionManager createAsyncConnectionManager() {
        Map<String, Object> sslConfigs = this.config.sslConfigs();
        String supportedProtocol = Optional.ofNullable(sslConfigs.get("ssl.protocol"))
                .map(Object::toString)
                .orElse(null);

        String supportedSuites = Optional.ofNullable(sslConfigs.get("cipher.suites"))
                .map(Object::toString)
                .orElse(null);
        Registry<TlsStrategy> tlsStrategyRegistry = null;
        if (StringUtil.isNotEmpty(supportedProtocol) && StringUtil.isNotEmpty(supportedSuites)) {
            DefaultClientTlsStrategy tlsStrategy = new DefaultClientTlsStrategy(
                    sslContext(),
                    new String[]{supportedProtocol},
                    new String[]{supportedSuites},
                    SSLBufferMode.STATIC,
                    getHostnameVerifier()
            );
            tlsStrategyRegistry = RegistryBuilder.<TlsStrategy>create()
                    .register(URIScheme.HTTPS.getId(), tlsStrategy)
                    .build();
        } else {
            DefaultClientTlsStrategy tlsStrategy = new DefaultClientTlsStrategy(
                    sslContext(),
                    getHostnameVerifier()
            );
            tlsStrategyRegistry = RegistryBuilder.<TlsStrategy>create()
                    .register(URIScheme.HTTPS.getId(), tlsStrategy)
                    .build();
        }

        return new PoolingAsyncClientConnectionManager(
                tlsStrategyRegistry,
                PoolConcurrencyPolicy.LAX,
                PoolReusePolicy.LIFO,
                TimeValue.ofHours(1)
        );
    }

    private RequestConfig createRequestConfig() {
        RequestConfig.Builder builder = RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.of(Duration.ofMillis(this.config.connectTimeoutMs())))
                .setResponseTimeout(Timeout.of(Duration.ofMillis(this.config.requestTimeoutMs())))
                .setMaxRedirects(config.maxRetries())
                .setConnectionKeepAlive(TimeValue.ofSeconds(30))
                .setContentCompressionEnabled(true);
        configureProxy(builder);
        return builder.build();
    }

    @SuppressWarnings("deprecation")
    private void configureProxy(RequestConfig.Builder requestConfigBuilder) {
        if (this.config.proxyEnabled()) {
            log.info("Establishing proxy host:port {}:{} for Http {}", this.config.proxyHost(),
                    this.config.proxyPort(), this.config.reqMethod());
            requestConfigBuilder.setProxy(new HttpHost(this.config.proxyHost(), this.config.proxyPort()));
        }
    }

    private SSLContext sslContext() {
        try {
            if (!this.config.sslEnabled()) {
                TrustStrategy ts = (x509Certificates, s) -> true;
                return SSLContexts.custom()
                        .loadTrustMaterial(ts)
                        .build();
            }
            log.info("Configuring SSL for this connection");
            Map<String, Object> sslConfigs = this.config.sslConfigs();
            SSLContextBuilder contextBuilder = SSLContexts.custom();
            Optional.ofNullable(sslConfigs.get("ssl.keystore.type"))
                    .map(Object::toString)
                    .ifPresent(contextBuilder::setKeyStoreType);
            addTrustMaterial(contextBuilder);
            addKeystoreContext(contextBuilder);
            return contextBuilder.build();
        } catch (IOException e) {
            throw new ConnectException("IOException configuring SSL", e);
        } catch (KeyManagementException e2) {
            throw new ConnectException("KeyManagement Exception configuring SSL", e2);
        } catch (KeyStoreException e3) {
            throw new ConnectException("KeyStore Exception configuring SSL", e3);
        } catch (NoSuchAlgorithmException e4) {
            throw new ConnectException("NoSuchAlgorithm Exception configuring SSL", e4);
        } catch (UnrecoverableKeyException e5) {
            throw new ConnectException("Unrecoverable Key Exception configuring SSL", e5);
        } catch (CertificateException e6) {
            throw new ConnectException("Certificate Exception configuring SSL", e6);
        }
    }

    private HostnameVerifier getHostnameVerifier() {
        boolean label = Optional.ofNullable(this.config.sslConfigs().get("ssl.endpoint.identification.algorithm"))
                .map(Object::toString)
                .map(String::isEmpty)
                .orElse(true);
        return label ? NoopHostnameVerifier.INSTANCE : new DefaultHostnameVerifier();
    }

    private void addTrustMaterial(SSLContextBuilder contextBuilder)
            throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
        String location = Optional.ofNullable(this.config.sslConfigs().get("ssl.truststore.location"))
                .map(Object::toString)
                .orElse(null);
        Password password = this.config.getPassword("https.ssl.truststore.password");
        if (StringUtil.isEmpty(location) || password == null) {
            contextBuilder.loadTrustMaterial((chain, authType) -> true);
        } else {
            contextBuilder.loadTrustMaterial(
                    new File(location),
                    password.value().toCharArray(),
                    (chain, authType) -> true);
        }
    }

    private void addKeystoreContext(SSLContextBuilder contextBuilder)
            throws IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException {
        Object keystoreLocationConfig = this.config.sslConfigs().get("ssl.keystore.location");
        Password kp = this.config.getPassword("https.ssl.keystore.password");
        Password keyPassword = this.config.getPassword("https.ssl.key.password");
        if (keystoreLocationConfig != null && kp != null && keyPassword != null) {
            contextBuilder.loadKeyMaterial(new File(keystoreLocationConfig.toString()),
                    kp.value().toCharArray(), keyPassword.value().toCharArray());
        }
    }

    private Optional<CredentialsProvider> credentialsProvider() {
        if (this.config.proxyEnabled() && !StringUtil.isEmpty(this.config.proxyUser())) {
            log.info("Establishing proxy credentials for this connection");
            BasicCredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(new AuthScope(this.config.proxyHost(), this.config.proxyPort()),
                    new UsernamePasswordCredentials(this.config.proxyUser(),
                            this.config.proxyPassword().value().toCharArray()));
            return Optional.of(credsProvider);
        }
        return Optional.empty();
    }

    public static class Http5RequestRetryStrategy extends DefaultHttpRequestRetryStrategy {
        public Http5RequestRetryStrategy(
                final int maxRetries,
                final TimeValue defaultRetryInterval,
                final Collection<Class<? extends IOException>> clazzes,
                final Collection<Integer> codes) {
            super(maxRetries, defaultRetryInterval, clazzes, codes);
        }
    }
}
