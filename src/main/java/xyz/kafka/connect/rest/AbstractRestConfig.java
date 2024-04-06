package xyz.kafka.connect.rest;

import com.alibaba.fastjson2.JSONPath;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.Method;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import xyz.kafka.connect.rest.enums.AuthType;
import xyz.kafka.connect.rest.enums.BehaviorOnError;
import xyz.kafka.connect.rest.enums.BehaviorOnNullValues;
import xyz.kafka.connect.rest.enums.ReportErrorAs;
import xyz.kafka.connect.rest.utils.HeaderConfigParser;
import xyz.kafka.connector.formatter.json.JsonFormatterConfig;
import xyz.kafka.connector.recommenders.Recommenders;
import xyz.kafka.connector.reporter.Reporter;
import xyz.kafka.connector.reporter.ReporterConfig;
import xyz.kafka.connector.validator.Validators;
import xyz.kafka.serialization.json.JsonDataConfig;
import xyz.kafka.utils.StringUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * AbstractRestConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-19
 */
public abstract class AbstractRestConfig extends AbstractConfig {
    public static final String REST_API_URL = "connection.url";
    private static final String REST_API_URL_DOC = "REST API URL.";
    private static final String REST_API_URL_DISPLAY = "REST URL";
    public static final String BEHAVIOR_ON_NULL_VALUES = "behavior.on.null.values";
    private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a non-null key and a null " +
            "value (i.e. Kafka tombstone records). Valid options are ``ignore``, ``delete``, ``log`` and ``fail``.";
    private static final String BEHAVIOR_ON_NULL_VALUES_DISPLAY = "Behavior for null valued records";
    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
    private static final String BEHAVIOR_ON_ERROR_DOC = """
            Error handling behavior setting for handling response from HTTP requests. Must be configured to one of the following:
            ``fail``
            Stops the connector when an error occurs.
            ``ignore``
            Continues to process next set of records. when error occurs.
            ``log``
            Logs the error message when error occurs and continues to process next set of records, available in ``${connector-name}-error`` topic""";
    private static final String BEHAVIOR_ON_ERRORS_DISPLAY = "Behavior On Errors";
    public static final String HEADERS = "headers";
    private static final String HEADERS_DOC = "REST headers to be included in all requests. Individual headers should be separated by the ``header.separator``";
    private static final String HEADERS_DISPLAY = "headers";
    public static final String HEADERS_DEFAULT = "";
    public static final String HEADER_SEPARATOR = "header.separator";
    private static final String HEADER_SEPARATOR_DOC = "Separator character used in headers property.";
    private static final String HEADER_SEPARATOR_DISPLAY = "header separator";
    public static final String HEADER_SEPARATOR_DEFAULT = ";";
    public static final String REQUEST_CONTENT_TYPE = "rest.request.content.type";
    public static final String REQUEST_CONTENT_TYPE_DOC = "HTTP content type";
    public static final String MAX_RETRIES = "max.retries";
    public static final int MAX_RETRIES_DEFAULT = 5;
    private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String RETRY_CODES = "retry.codes";
    private static final String RETRY_CODES_DOC = "Retry codes";
    private static final String RETRY_CODES_DISPLAY = RETRY_CODES_DOC;

    public static final String BATCH_JSON_AS_ARRAY_DOC = "Send individual messages in a JSON array.";
    public static final String BATCH_JSON_AS_ARRAY_DISPLAY = "JSON as arrays";
    public static final String BATCH_MAX_SIZE = "batch.max.size";
    public static final int BATCH_MAX_SIZE_DEFAULT = 1;
    private static final String BATCH_MAX_SIZE_DOC = "The number of records accumulated in a batch before the HTTP API is invoked.";
    private static final String BATCH_MAX_SIZE_DISPLAY = "Maximum batch size";
    public static final String BATCH_PREFIX = "batch.prefix";
    public static final String BATCH_PREFIX_DEFAULT = "";
    private static final String BATCH_PREFIX_DOC = "Prefix added to record batches. This is applied once at the beginning of the batch of records.";
    private static final String BATCH_PREFIX_DISPLAY = "Batch prefix";
    public static final String BATCH_SUFFIX = "batch.suffix";
    public static final String BATCH_SUFFIX_DEFAULT = "";
    private static final String BATCH_SUFFIX_DOC = "Suffix added to record batches. This is applied once at the end of the batch of records.";
    private static final String BATCH_SUFFIX_DISPLAY = "Batch suffix";
    public static final String BATCH_SEPARATOR = "batch.separator";
    public static final String BATCH_SEPARATOR_DEFAULT = ",";
    private static final String BATCH_SEPARATOR_DOC = "Separator for records in a batch.";
    private static final String BATCH_SEPARATOR_DISPLAY = "Batch separator";
    public static final String PROXY_HOST = "rest.proxy.host";
    public static final String PROXY_HOST_DEFAULT = "";
    public static final String PROXY_HOST_DOC = "The host or ip of the http proxy.";
    public static final String PROXY_HOST_DISPLAY = "Proxy host";
    public static final String PROXY_PORT = "rest.proxy.port";
    public static final int PROXY_PORT_DEFAULT = 0;
    public static final String PROXY_PORT_DOC = "The port number of the http proxy.";
    public static final String PROXY_PORT_DISPLAY = "Proxy port";
    public static final String PROXY_USER = "rest.proxy.user";
    public static final String PROXY_USER_DEFAULT = "";
    public static final String PROXY_USER_DOC = "The username to be used when authenticating with the http proxy.";
    public static final String PROXY_USER_DISPLAY = "Proxy username";
    public static final String PROXY_PASSWORD = "rest.proxy.password";
    public static final String PROXY_PASSWORD_DEFAULT = "";
    public static final String PROXY_PASSWORD_DOC = "The password to be used when authenticating with the http proxy.";
    public static final String PROXY_PASSWORD_DISPLAY = "Proxy password";
    public static final String CONNECT_TIMEOUT_MS = "rest.connect.timeout.ms";
    public static final String CONNECT_TIMEOUT_MS_DOC = "Time to wait for a connection to be established.";
    public static final String CONNECT_TIMEOUT_MS_DISPLAY = "HTTP Connect Timeout (ms)";
    public static final String REQUEST_TIMEOUT_MS = "rest.request.timeout.ms";
    public static final String REQUEST_TIMEOUT_MS_DOC = "Time to wait for a request response to arrive.";
    public static final String REQUEST_TIMEOUT_MS_DISPLAY = "HTTP Request Timeout (ms)";
    public static final String AUTH_TYPE = "auth.type";
    private static final String AUTH_TYPE_DOC = "Authentication type of the endpoint. Valid values  are ``NONE``, ``BASIC``, ``OAUTH2``(Client Credentials grant type only), ``THIRD_PARTY`` .";
    private static final String AUTH_TYPE_DISPLAY = "Endpoint Authentication type";

    public static final String AUTH_EXPIRES_IN_SECONDS = "auth.expires_in_seconds";
    private static final long AUTH_EXPIRES_IN_SECONDS_DEFAULT = 3600;
    private static final String AUTH_EXPIRES_IN_SECONDS_DOC = "Authentication expires in seconds.";
    private static final String AUTH_EXPIRES_IN_SECONDS_DISPLAY = "Authentication expires in seconds.";
    public static final String AUTH_USERNAME = "connection.user";
    private static final String AUTH_USERNAME_DEFAULT = "";
    private static final String AUTH_USERNAME_DOC = "The username to be used with an endpoint requiring authentication.";
    private static final String AUTH_USERNAME_DISPLAY = "Auth username";
    public static final String AUTH_PASSWORD = "connection.password";
    private static final String AUTH_PASSWORD_DEFAULT = "";
    private static final String AUTH_PASSWORD_DOC = "The password to be used with an endpoint requiring authentication.";
    private static final String AUTH_PASSWORD_DISPLAY = "Auth password";
    public static final String OAUTH_TOKEN_URL = "oauth2.token.url";
    private static final String OAUTH_TOKEN_URL_DEFAULT = "";
    private static final String OAUTH_TOKEN_URL_DOC = "The URL to be used for fetching OAuth2 token. Client Credentials is the only supported grant type.";
    private static final String OAUTH_TOKEN_URL_DISPLAY = "OAuth2 token url";
    public static final String OAUTH_CLIENT_ID = "oauth2.client.id";
    private static final String OAUTH_CLIENT_ID_DEFAULT = "";
    private static final String OAUTH_CLIENT_ID_DOC = "The client id used when fetching OAuth2 token";
    private static final String OAUTH_CLIENT_ID_DISPLAY = "OAuth2 client id";
    public static final String OAUTH_CLIENT_SCOPE = "oauth2.client.scope";
    private static final String OAUTH_CLIENT_SCOPE_DEFAULT = "any";
    private static final String OAUTH_CLIENT_SCOPE_DOC = "The scope used when fetching OAuth2 token";
    private static final String OAUTH_CLIENT_SCOPE_DISPLAY = "OAuth2 scope. If empty, this parameter is not set in the authorization request.";
    public static final String OAUTH_CLIENT_SECRET = "oauth2.client.secret";
    private static final String OAUTH_CLIENT_SECRET_DEFAULT = "";
    private static final String OAUTH_CLIENT_SECRET_DOC = "The secret used when fetching OAuth2 token";
    private static final String OAUTH_CLIENT_SECRET_DISPLAY = "OAuth2 secret";
    public static final String OAUTH_TOKEN_PATH = "oauth2.token.json.path";
    public static final String OAUTH_TOKEN_PATH_DEFAULT = "$.access_token";
    public static final String OAUTH_TOKEN_PATH_DOC = "The JSON path of the property containing the OAuth2 token returned by the http proxy. Default value is ``access_token``.";
    public static final String OAUTH_TOKEN_PATH_DISPLAY = "OAuth2 token property name";
    public static final String OAUTH_CLIENT_AUTH_MODE = "oauth2.client.auth.mode";
    public static final String OAUTH_CLIENT_AUTH_MODE_DEFAULT = "header";
    public static final String OAUTH_CLIENT_AUTH_MODE_DOC = "Specifies how to encode ``client_id`` and ``client_secret`` in the OAuth2 authorization request. If set to 'header', the credentials are encoded as an ``'Authorization: Basic <base-64 encoded client_id:client_secret>'`` HTTP header. If set to 'url', then ``client_id`` and ``client_secret`` are sent as URL encoded parameters.";
    public static final String OAUTH_CLIENT_AUTH_MODE_DISPLAY = "OAuth2 auth mode mechanism";
    public static final String CLIENT_AUTH_MODE_HEADER = "header";
    public static final String CLIENT_AUTH_MODE_URL = "url";
    public static final String OAUTH_CLIENT_HEADERS = "oauth2.client.headers";
    public static final String OAUTH_CLIENT_HEADERS_DOC = "HTTP headers to be included in each request to the OAuth2 client endpoint. Individual headers should be separated by `oauth2.client.header.separator`. Note: 'Content-Type` header if added will be overridden by the connector with value 'application/x-www-form-urlencoded'";
    private static final String OAUTH_CLIENT_HEADERS_DISPLAY = "OAuth2 Client Headers";
    public static final String OAUTH_CLIENT_HEADERS_DEFAULT = "";
    public static final String OAUTH_CLIENT_HEADER_SEPARATOR = "oauth2.client.header.separator";
    private static final String OAUTH_CLIENT_HEADER_SEPARATOR_DOC = "Separator character used in `oauth2.client.headers` property.";
    private static final String OAUTH_CLIENT_HEADER_SEPARATOR_DISPLAY = "OAuth2 Header Separator";
    public static final String OAUTH_CLIENT_HEADER_SEPARATOR_DEFAULT = "|";

    public static final String THIRD_PARTY_TOKEN_REQ_HEADERS = "third.party.token.req.headers";

    public static final String THIRD_PARTY_TOKEN_REQ_BODY = "third.party.token.req.body";

    public static final String THIRD_PARTY_TOKEN_ENDPOINT = "third.party.token.endpoint";

    public static final String THIRD_PARTY_ACCESS_TOKEN_JSON_PATH = "third.party.access.token.json.path";

    public static final String THIRD_PARTY_AUTHORIZATION_HEADER_NAME = "third.party.authorization.header.name";

    public static final String THIRD_PARTY_AUTHORIZATION_HEADER_PREFIX = "third.party.authorization.header.prefix";
    public static final String REPORT_ERRORS_AS_PROPERTY = "report.errors.as";
    public static final String REPORT_ERRORS_AS_DOC = "Dictates the content of records produced to the error topic. If set to ``error_string``, the value would be a human readable string describing the failure. The value will include some or all of the following information if available: http response code, reason phrase, submitted payload, url, response content, exception and error message.\nIf set to http_response, the value would be the plain response content for the request which failed to write the record. \nIn both modes, any information about the failure will also be included in the error records headers. \n";
    public static final String REPORT_ERRORS_AS_DISPLAY = "Report errors as";
    public static final String CONNECTION_SSL_CONFIG_PREFIX = "https.";
    private static final String CONNECTION_GROUP = "Connection";
    private static final String RETRIES_GROUP = "Retries";
    private static final String BATCHING_GROUP = "Batching";
    private static final String SSL_GROUP = "Security";
    private static final String AUTH_GROUP = "Authorization";
    private static final String ON_FAILURE_GROUP = "On Failure";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC =
            "The number of records to process as a batch when writing to Http api.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";
    private static final int BATCH_SIZE_DEFAULT = 2000;

    public static final String BATCH_JSON_AS_ARRAY = "batch.json.as.array";
    public static final boolean BATCH_JSON_AS_ARRAY_DEFAULT = true;

    private static final String BEHAVIOR_ON_NULL_VALUES_DEFAULT = BehaviorOnNullValues.IGNORE.toString().toLowerCase();
    private static final String BEHAVIOR_ON_ERROR_DEFAULT = BehaviorOnError.FAIL.name().toLowerCase();
    public static final String REQUEST_BODY_FORMAT_DEFAULT = RecordFormat.STRING.toString();
    public static final int RETRY_BACKOFF_MS_DEFAULT = Math.toIntExact(TimeUnit.SECONDS.toMillis(3));
    public static final int CONNECT_TIMEOUT_MS_DEFAULT = Math.toIntExact(TimeUnit.SECONDS.toMillis(5));
    public static final int REQUEST_TIMEOUT_MS_DEFAULT = Math.toIntExact(TimeUnit.SECONDS.toMillis(5));
    private static final String AUTH_TYPE_DEFAULT = AuthType.NONE.toString();
    public static final String REPORT_ERRORS_AS_DEFAULT = ReportErrorAs.ERROR_STRING.name().toLowerCase();
    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(1);
    private final boolean batchJsonAsArray;
    private final String restApiUrl;
    private final BehaviorOnNullValues behaviorOnNullValues;
    private final int maxRetries;
    private final int retryBackoffMs;
    private final List<Integer> retryCodes;
    private final int connectTimeoutMs;
    private final int requestTimeoutMs;
    private final List<Header> headers;
    private final String headerSeparator;
    private final String contentType;
    private final String batchSeparator;
    private final String batchPrefix;
    private final String batchSuffix;

    private final BehaviorOnError behaviorOnError;

    private final String proxyHost;
    private final int proxyPort;
    private final String proxyUser;
    private final Password proxyPassword;
    private final AuthType authType;
    private final long authExpiresInSeconds;
    private final String authUsername;
    private final Password authPassword;
    private final String oauthTokenUrl;
    private final String oauthClientId;
    private final String oauthClientScope;
    private final String oauthSecretsEncoding;
    private final Password oauthClientSecret;
    private final JSONPath oauthTokenPath;
    private final String oauthClientHeaders;
    private final String oauthClientHeaderSeparator;
    private String thirdPartyTokenReqBody;
    private final String thirdPartyTokenEndpoint;
    private List<Header> thirdPartyTokenReqHeaders;
    private JSONPath thirdPartyAccessTokenPointer;
    private String thirdPartyAuthorizationHeaderName;
    private String thirdPartyAuthorizationHeaderPrefix;
    private final ReportErrorAs reportErrorAs;
    private Reporter reporter;
    private ReporterConfig reporterConfig;
    private final JsonFormatterConfig jsonFormatterConfig;
    private final Map<String, Object> sslConfigs;
    private final boolean proxyEnabled;
    private final boolean sslEnabled;
    private final int batchSize;
    private final JsonDataConfig jsonDataConfig;

    protected AbstractRestConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals);
        this.restApiUrl = this.getString(REST_API_URL);
        this.jsonFormatterConfig = new JsonFormatterConfig(
                originalsWithPrefix(AbstractRestConfig.RecordFormat.JSON.name().toLowerCase() + "."));
        ConfigDef sslConfigDef = new ConfigDef();
        SslConfigs.addClientSslSupport(sslConfigDef);
        this.batchSize = getInt(BATCH_SIZE_CONFIG);
        this.behaviorOnNullValues = BehaviorOnNullValues.valueOf(getString(BEHAVIOR_ON_NULL_VALUES).toUpperCase());
        this.maxRetries = getInt(MAX_RETRIES);
        this.behaviorOnError = BehaviorOnError.valueOf(getString(BEHAVIOR_ON_ERROR).toUpperCase());
        this.retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        this.retryCodes = getList(RETRY_CODES)
                .stream()
                .map(StringUtil::toInteger)
                .toList();
        this.connectTimeoutMs = getInt(CONNECT_TIMEOUT_MS);
        this.requestTimeoutMs = getInt(REQUEST_TIMEOUT_MS);
        this.headerSeparator = getString(HEADER_SEPARATOR);
        this.contentType = getString(REQUEST_CONTENT_TYPE);
        HeaderConfigParser headerConfigParser = HeaderConfigParser.getInstance();
        this.headers = headerConfigParser.parseHeadersConfig(
                HEADERS,
                this.getString(HEADERS),
                this.headerSeparator
        ).stream().toList();
        this.batchSeparator = getString(BATCH_SEPARATOR);
        this.batchPrefix = getString(BATCH_PREFIX);
        this.batchSuffix = getString(BATCH_SUFFIX);
        this.proxyHost = getString(PROXY_HOST);
        this.proxyPort = getInt(PROXY_PORT);
        this.proxyUser = getString(PROXY_USER);
        this.proxyPassword = getPassword(PROXY_PASSWORD);
        this.authType = AuthType.valueOf(getString(AUTH_TYPE).toUpperCase());
        this.authExpiresInSeconds = getLong(AUTH_EXPIRES_IN_SECONDS);
        this.authUsername = getString(AUTH_USERNAME);
        this.authPassword = getPassword(AUTH_PASSWORD);
        this.oauthTokenUrl = getString(OAUTH_TOKEN_URL);
        this.oauthClientId = getString(OAUTH_CLIENT_ID);
        this.oauthClientScope = getString(OAUTH_CLIENT_SCOPE);
        this.oauthSecretsEncoding = getString(OAUTH_CLIENT_AUTH_MODE).toLowerCase();
        this.oauthClientSecret = getPassword(OAUTH_CLIENT_SECRET);
        this.oauthTokenPath = JSONPath.of(getString(OAUTH_TOKEN_PATH));
        this.oauthClientHeaders = getString(OAUTH_CLIENT_HEADERS);
        this.oauthClientHeaderSeparator = getString(OAUTH_CLIENT_HEADER_SEPARATOR);
        this.thirdPartyTokenEndpoint = getString(THIRD_PARTY_TOKEN_ENDPOINT);
        if (this.thirdPartyTokenEndpoint != null) {
            this.thirdPartyTokenReqBody = getPassword(THIRD_PARTY_TOKEN_REQ_BODY).value();
            this.thirdPartyTokenReqHeaders = headerConfigParser.parseHeadersConfig(
                    THIRD_PARTY_TOKEN_REQ_HEADERS,
                    getString(THIRD_PARTY_TOKEN_REQ_HEADERS),
                    this.headerSeparator
            ).stream().toList();
            this.thirdPartyAccessTokenPointer = JSONPath.of(getString(THIRD_PARTY_ACCESS_TOKEN_JSON_PATH));
            this.thirdPartyAuthorizationHeaderName = getString(THIRD_PARTY_AUTHORIZATION_HEADER_NAME);
            this.thirdPartyAuthorizationHeaderPrefix = getString(THIRD_PARTY_AUTHORIZATION_HEADER_PREFIX);
        }

        this.reportErrorAs = ReportErrorAs.valueOf(getString(REPORT_ERRORS_AS_PROPERTY).toUpperCase());
        if (originals.containsKey("reporter.result.topic.name")) {
            this.reporterConfig = new ReporterConfig(
                    (String) originals().get("name"),
                    null,
                    null,
                    originalsWithPrefix("reporter.")
            );
            this.reporter = new Reporter();
            this.reporter.configure(reporterConfig);
        }
        this.sslConfigs = Collections.unmodifiableMap(
                sslConfigDef.parse(originalsWithPrefix(CONNECTION_SSL_CONFIG_PREFIX))
        );
        this.proxyEnabled = !StringUtil.isEmpty(proxyHost) && proxyPort > 0;
        this.sslEnabled = sslProtocol(restApiUrl) || sslProtocol(oauthTokenUrl);
        this.jsonDataConfig = new JsonDataConfig(originals);
        this.batchJsonAsArray = getBoolean(BATCH_JSON_AS_ARRAY);
    }

    public abstract Method reqMethod();

    private static void addConnectionGroup(ConfigDef configDef) {
        int order = 0;
        configDef.define(
                REST_API_URL,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Validators.validUri("http", "https"),
                ConfigDef.Importance.HIGH,
                REST_API_URL_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.LONG,
                REST_API_URL_DISPLAY
        ).define(
                BEHAVIOR_ON_NULL_VALUES,
                ConfigDef.Type.STRING,
                BEHAVIOR_ON_NULL_VALUES_DEFAULT,
                Validators.oneOf(BehaviorOnNullValues.class),
                ConfigDef.Importance.LOW,
                BEHAVIOR_ON_NULL_VALUES_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                BEHAVIOR_ON_NULL_VALUES_DISPLAY,
                Recommenders.enumValues(BehaviorOnNullValues.class)
        ).define(
                BEHAVIOR_ON_ERROR,
                ConfigDef.Type.STRING,
                BEHAVIOR_ON_ERROR_DEFAULT,
                Validators.oneOf(BehaviorOnError.class),
                ConfigDef.Importance.LOW,
                BEHAVIOR_ON_ERROR_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                BEHAVIOR_ON_ERRORS_DISPLAY,
                Recommenders.enumValues(BehaviorOnError.class)
        ).define(
                HEADERS,
                ConfigDef.Type.STRING,
                HEADERS_DEFAULT,
                ConfigDef.Importance.HIGH,
                HEADERS_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                HEADERS_DISPLAY
        ).define(
                HEADER_SEPARATOR,
                ConfigDef.Type.STRING,
                HEADER_SEPARATOR_DEFAULT,
                ConfigDef.Importance.HIGH,
                HEADER_SEPARATOR_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                HEADER_SEPARATOR_DISPLAY
        ).define(
                REQUEST_CONTENT_TYPE,
                ConfigDef.Type.STRING,
                null,
                Validators.nonEmptyString(),
                ConfigDef.Importance.HIGH,
                REQUEST_CONTENT_TYPE_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                REQUEST_CONTENT_TYPE_DOC
        ).define(
                PROXY_HOST,
                ConfigDef.Type.STRING,
                PROXY_HOST_DEFAULT,
                ConfigDef.Importance.HIGH,
                PROXY_HOST_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                PROXY_HOST_DISPLAY
        ).define(
                PROXY_PORT,
                ConfigDef.Type.INT,
                PROXY_PORT_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.HIGH,
                PROXY_PORT_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                PROXY_PORT_DISPLAY
        ).define(
                PROXY_USER,
                ConfigDef.Type.STRING,
                PROXY_USER_DEFAULT,
                ConfigDef.Importance.HIGH,
                PROXY_USER_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                PROXY_USER_DISPLAY
        ).define(
                PROXY_PASSWORD,
                ConfigDef.Type.PASSWORD,
                PROXY_PASSWORD_DEFAULT,
                ConfigDef.Importance.HIGH,
                PROXY_PASSWORD_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                PROXY_PASSWORD_DISPLAY
        ).define(
                CONNECT_TIMEOUT_MS,
                ConfigDef.Type.INT,
                CONNECT_TIMEOUT_MS_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                CONNECT_TIMEOUT_MS_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                CONNECT_TIMEOUT_MS_DISPLAY
        ).define(
                REQUEST_TIMEOUT_MS,
                ConfigDef.Type.INT,
                REQUEST_TIMEOUT_MS_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                REQUEST_TIMEOUT_MS_DOC,
                CONNECTION_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                REQUEST_TIMEOUT_MS_DISPLAY
        ).define(
                BATCH_SIZE_CONFIG,
                ConfigDef.Type.INT,
                BATCH_SIZE_DEFAULT,
                between(1, 1000000),
                ConfigDef.Importance.MEDIUM,
                BATCH_SIZE_DOC,
                CONNECTION_GROUP,
                ++order,
                ConfigDef.Width.SHORT,
                BATCH_SIZE_DISPLAY
        ).define(
                BATCH_JSON_AS_ARRAY,
                ConfigDef.Type.BOOLEAN,
                BATCH_JSON_AS_ARRAY_DEFAULT,
                ConfigDef.Importance.HIGH,
                BATCH_JSON_AS_ARRAY_DOC,
                CONNECTION_GROUP,
                ++order,
                ConfigDef.Width.SHORT,
                BATCH_JSON_AS_ARRAY_DISPLAY
        );
    }

    private static void addAuthGroup(ConfigDef configDef) {
        int order = 0;

        configDef.define(
                AUTH_TYPE,
                ConfigDef.Type.STRING,
                AUTH_TYPE_DEFAULT,
                ConfigDef.Importance.HIGH,
                AUTH_TYPE_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                AUTH_TYPE_DISPLAY,
                Recommenders.enumValues(AuthType.class)
        ).define(
                AUTH_EXPIRES_IN_SECONDS,
                ConfigDef.Type.LONG,
                AUTH_EXPIRES_IN_SECONDS_DEFAULT,
                ConfigDef.Importance.HIGH,
                AUTH_EXPIRES_IN_SECONDS_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                AUTH_EXPIRES_IN_SECONDS_DISPLAY
        ).define(
                AUTH_USERNAME,
                ConfigDef.Type.STRING,
                AUTH_USERNAME_DEFAULT,
                ConfigDef.Importance.HIGH,
                AUTH_USERNAME_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                AUTH_USERNAME_DISPLAY
        ).define(
                AUTH_PASSWORD,
                ConfigDef.Type.PASSWORD,
                AUTH_PASSWORD_DEFAULT,
                ConfigDef.Importance.HIGH,
                AUTH_PASSWORD_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                AUTH_PASSWORD_DISPLAY
        ).define(
                OAUTH_TOKEN_URL,
                ConfigDef.Type.STRING,
                OAUTH_TOKEN_URL_DEFAULT,
                ConfigDef.Importance.HIGH,
                OAUTH_TOKEN_URL_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                OAUTH_TOKEN_URL_DISPLAY
        ).define(
                OAUTH_CLIENT_ID,
                ConfigDef.Type.STRING,
                OAUTH_CLIENT_ID_DEFAULT,
                ConfigDef.Importance.HIGH,
                OAUTH_CLIENT_ID_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                OAUTH_CLIENT_ID_DISPLAY
        ).define(
                OAUTH_CLIENT_SECRET,
                ConfigDef.Type.PASSWORD,
                OAUTH_CLIENT_SECRET_DEFAULT,
                ConfigDef.Importance.HIGH,
                OAUTH_CLIENT_SECRET_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                OAUTH_CLIENT_SECRET_DISPLAY
        ).define(
                OAUTH_TOKEN_PATH,
                ConfigDef.Type.STRING,
                OAUTH_TOKEN_PATH_DEFAULT,
                ConfigDef.Importance.HIGH,
                OAUTH_TOKEN_PATH_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                OAUTH_TOKEN_PATH_DISPLAY
        ).define(
                OAUTH_CLIENT_AUTH_MODE,
                ConfigDef.Type.STRING,
                OAUTH_CLIENT_AUTH_MODE_DEFAULT,
                Validators.oneOf(CLIENT_AUTH_MODE_HEADER, CLIENT_AUTH_MODE_URL),
                ConfigDef.Importance.LOW, OAUTH_CLIENT_AUTH_MODE_DOC, AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                OAUTH_CLIENT_AUTH_MODE_DISPLAY
        ).define(
                OAUTH_CLIENT_SCOPE,
                ConfigDef.Type.STRING,
                OAUTH_CLIENT_SCOPE_DEFAULT,
                ConfigDef.Importance.LOW,
                OAUTH_CLIENT_SCOPE_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                OAUTH_CLIENT_SCOPE_DISPLAY
        ).define(
                OAUTH_CLIENT_HEADERS,
                ConfigDef.Type.STRING,
                OAUTH_CLIENT_HEADERS_DEFAULT,
                ConfigDef.Importance.LOW,
                OAUTH_CLIENT_HEADERS_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.MEDIUM,
                OAUTH_CLIENT_HEADERS_DISPLAY
        ).define(
                OAUTH_CLIENT_HEADER_SEPARATOR,
                ConfigDef.Type.STRING,
                OAUTH_CLIENT_HEADER_SEPARATOR_DEFAULT,
                ConfigDef.Importance.LOW,
                OAUTH_CLIENT_HEADER_SEPARATOR_DOC,
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                OAUTH_CLIENT_HEADER_SEPARATOR_DISPLAY
        ).define(
                THIRD_PARTY_TOKEN_REQ_HEADERS,
                STRING,
                null,
                ConfigDef.Importance.LOW,
                "Request header list used when fetching third party token",
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                "Request header list used when fetching third party token"
        ).define(
                THIRD_PARTY_TOKEN_REQ_BODY,
                PASSWORD,
                null,
                ConfigDef.Importance.LOW,
                "Request body used when fetching third party token",
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                "Request body used when fetching third party token"
        ).define(
                THIRD_PARTY_TOKEN_ENDPOINT,
                STRING,
                null,
                ConfigDef.Importance.LOW,
                "HTTP endpoint used when fetching third party token",
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                "HTTP endpoint used when fetching third party token"
        ).define(
                THIRD_PARTY_ACCESS_TOKEN_JSON_PATH,
                STRING,
                null,
                ConfigDef.Importance.LOW,
                "The json path of access token used when parse third party token",
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                "The json path of access token used when parse third party token"
        ).define(
                THIRD_PARTY_AUTHORIZATION_HEADER_NAME,
                STRING,
                null,
                ConfigDef.Importance.LOW,
                "Third party authorization header name",
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                "Third party authorization header name"
        ).define(
                THIRD_PARTY_AUTHORIZATION_HEADER_PREFIX,
                STRING,
                "OAuth2",
                ConfigDef.Importance.LOW,
                "Third party authorization header prefix",
                AUTH_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                "Third party authorization header prefix"
        );
    }

    private static void addRetriesGroup(ConfigDef configDef) {
        int order = 0;
        configDef.define(
                MAX_RETRIES,
                ConfigDef.Type.INT,
                MAX_RETRIES_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                MAX_RETRIES_DOC,
                RETRIES_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                MAX_RETRIES_DISPLAY
        ).define(
                RETRY_BACKOFF_MS,
                ConfigDef.Type.INT,
                RETRY_BACKOFF_MS_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                RETRY_BACKOFF_MS_DOC,
                RETRIES_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                RETRY_BACKOFF_MS_DISPLAY
        ).define(
                RETRY_CODES,
                ConfigDef.Type.LIST,
                List.of(),
                ConfigDef.Importance.MEDIUM,
                RETRY_CODES_DOC,
                RETRIES_GROUP,
                order++,
                ConfigDef.Width.SHORT,
                RETRY_CODES_DISPLAY
        );
    }

    private static void addOnFailureGroup(ConfigDef configDef) {
        configDef.define(
                REPORT_ERRORS_AS_PROPERTY,
                ConfigDef.Type.STRING,
                REPORT_ERRORS_AS_DEFAULT,
                Validators.oneOf(ReportErrorAs.class),
                ConfigDef.Importance.MEDIUM,
                REPORT_ERRORS_AS_DOC, ON_FAILURE_GROUP,
                1,
                ConfigDef.Width.SHORT,
                REPORT_ERRORS_AS_DISPLAY,
                Recommenders.visibleIf("reporter.error.topic.name",
                        Validators.anyOf(Validators.nonEmptyString(), Validators.notNull()),
                        Recommenders.enumValues(ReportErrorAs.class)));
    }

    private static void addBatchingGroup(ConfigDef configDef) {
        configDef.define(
                BATCH_MAX_SIZE,
                ConfigDef.Type.INT,
                BATCH_MAX_SIZE_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.HIGH,
                BATCH_MAX_SIZE_DOC,
                BATCHING_GROUP,
                3,
                ConfigDef.Width.SHORT,
                BATCH_MAX_SIZE_DISPLAY
        ).define(
                BATCH_PREFIX,
                ConfigDef.Type.STRING,
                BATCH_PREFIX_DEFAULT,
                ConfigDef.Importance.HIGH,
                BATCH_PREFIX_DOC,
                BATCHING_GROUP,
                4,
                ConfigDef.Width.SHORT,
                BATCH_PREFIX_DISPLAY
        ).define(
                BATCH_SUFFIX,
                ConfigDef.Type.STRING,
                BATCH_SUFFIX_DEFAULT,
                ConfigDef.Importance.HIGH,
                BATCH_SUFFIX_DOC,
                BATCHING_GROUP,
                5,
                ConfigDef.Width.SHORT,
                BATCH_SUFFIX_DISPLAY
        ).define(
                BATCH_SEPARATOR,
                ConfigDef.Type.STRING,
                BATCH_SEPARATOR_DEFAULT,
                ConfigDef.Importance.HIGH,
                BATCH_SEPARATOR_DOC,
                BATCHING_GROUP,
                6,
                ConfigDef.Width.SHORT,
                BATCH_SEPARATOR_DISPLAY
        );
    }

    public static ConfigDef conf() {
        ConfigDef configDef = new ConfigDef();
        addConnectionGroup(configDef);
        addAuthGroup(configDef);
        addRetriesGroup(configDef);
        addBatchingGroup(configDef);
        addOnFailureGroup(configDef);
        ConfigDef sslConfigDef = new ConfigDef();
        SslConfigs.addClientSslSupport(sslConfigDef);
        configDef.embed(CONNECTION_SSL_CONFIG_PREFIX, SSL_GROUP, configDef.configKeys().size() + 1, sslConfigDef);
        return configDef;
    }

    public enum RecordFormat {
        /**
         *
         */
        STRING,
        JSON;

        public static RecordFormat valueOfIgnoreCase(String value) {
            return Arrays.stream(values())
                    .filter(rf -> rf.toString().equalsIgnoreCase(value))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Record format '%s' not found (%s)",
                            value, asStringList())));
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.getDefault());
        }

        public static List<String> asStringList() {
            return Arrays.stream(values()).map(RecordFormat::toString)
                    .toList();
        }
    }

    private static String getProtocol(String url) throws MalformedURLException {
        return new URL(url).getProtocol();
    }

    private boolean sslProtocol(String url) {
        try {
            return "https".equalsIgnoreCase(getProtocol(url));
        } catch (MalformedURLException e) {
            return false;
        }
    }

    public final String restApiUrl() {
        return restApiUrl;
    }

    public final BehaviorOnNullValues behaviorOnNullValues() {
        return behaviorOnNullValues;
    }

    public final int maxRetries() {
        return maxRetries;
    }

    public final int retryBackoffMs() {
        return retryBackoffMs;
    }

    public final List<Integer> retryCodes() {
        return retryCodes;
    }

    public final int connectTimeoutMs() {
        return connectTimeoutMs;
    }

    public final int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    public final List<Header> headers() {
        return headers;
    }

    public final String headerSeparator() {
        return headerSeparator;
    }

    public final String contentType() {
        return contentType;
    }

    public final String batchSeparator() {
        return batchSeparator;
    }

    public final String batchPrefix() {
        return batchPrefix;
    }

    public final String batchSuffix() {
        return batchSuffix;
    }

    public final BehaviorOnError behaviorOnError() {
        return behaviorOnError;
    }

    public final String proxyHost() {
        return proxyHost;
    }

    public final int proxyPort() {
        return proxyPort;
    }

    public final String proxyUser() {
        return proxyUser;
    }

    public final Password proxyPassword() {
        return proxyPassword;
    }

    public final AuthType authType() {
        return authType;
    }

    public final long authExpiresInSeconds() {
        return authExpiresInSeconds;
    }

    public final String authUsername() {
        return authUsername;
    }

    public final Password authPassword() {
        return authPassword;
    }

    public final String oauthTokenUrl() {
        return oauthTokenUrl;
    }

    public final String oauthClientId() {
        return oauthClientId;
    }

    public final String oauthClientScope() {
        return oauthClientScope;
    }

    public final String oauthSecretsEncoding() {
        return oauthSecretsEncoding;
    }

    public final Password oauthClientSecret() {
        return oauthClientSecret;
    }

    public final JSONPath oauthTokenPath() {
        return oauthTokenPath;
    }

    public final String oauthClientHeaders() {
        return oauthClientHeaders;
    }

    public final String oauthClientHeaderSeparator() {
        return oauthClientHeaderSeparator;
    }

    public String thirdPartyTokenReqBody() {
        return thirdPartyTokenReqBody;
    }

    public String thirdPartyTokenEndpoint() {
        return thirdPartyTokenEndpoint;
    }

    public List<Header> thirdPartyTokenReqHeaders() {
        return thirdPartyTokenReqHeaders;
    }

    public JSONPath thirdPartyAccessTokenPointer() {
        return thirdPartyAccessTokenPointer;
    }

    public String thirdPartyAuthorizationHeaderName() {
        return thirdPartyAuthorizationHeaderName;
    }

    public String thirdPartyAuthorizationHeaderPrefix() {
        return thirdPartyAuthorizationHeaderPrefix;
    }

    public final ReportErrorAs reportErrorAs() {
        return reportErrorAs;
    }

    public Reporter reporter() {
        return reporter;
    }

    public ReporterConfig reporterConfig() {
        return reporterConfig;
    }

    public final JsonFormatterConfig jsonFormatterConfig() {
        return jsonFormatterConfig;
    }

    public final Map<String, Object> sslConfigs() {
        return sslConfigs;
    }

    public final boolean proxyEnabled() {
        return proxyEnabled;
    }

    public final boolean sslEnabled() {
        return sslEnabled;
    }

    public final int batchSize() {
        return batchSize;
    }

    public final JsonDataConfig jsonDataConfig() {
        return jsonDataConfig;
    }

    public boolean batchJsonAsArray() {
        return batchJsonAsArray;
    }
}
