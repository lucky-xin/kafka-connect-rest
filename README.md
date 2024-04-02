# Kafka Connect Rest

## kafka 消息推送到Http API

### xyz.kafka.connect.rest.sink.RestSinkConnector配置如下

```json
{
  "connector.class": "xyz.kafka.connect.rest.sink.RestSinkConnector",
  "record.key.field": "time",
  "behavior.on.null.values": "ignore",
  "oauth2.token.property": "$.access_token",
  "errors.log.include.messages": "true",
  "connection.url": "http://dingtalk-hook.piston-alert.svc.cluster.local:6666/message/markdown",
  "oauth2.client.secret": "******",
  "transforms": "Drop,ValueToKey,BloomFilter",
  "value.converter.decimal.format": "NUMERIC",
  "value.converter.json.fail.invalid.schema": "false",
  "errors.deadletterqueue.context.headers.enable": "true",
  "rate.limit": "1000",
  "batch.json.as.array": "false",
  "oauth2.client.auth.mode": "header",
  "errors.deadletterqueue.topic.replication.factor": "8",
  "transforms.Drop.condition": "valueSchema.fields.keySet().containsAll(['container_name','service_name','message','level','@timestamp','namespace_name']) && !value.container_name.startsWith('it-ucar-data') && (currTimeMills - value.'@timestamp'.getTime() < 300000) && value.level == 'ERROR' && value.namespace_name == 'piston-cloud'",
  "errors.log.enable": "true",
  "key.converter": "com.pistonint.kafka.connect.convert.json.JsonConverter",
  "transforms.ValueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
  "auth.type": "oauth2",
  "errors.retry.timeout": "600000",
  "value.converter.latest.compatibility.strict": "false",
  "behavior.on.error": "log",
  "transforms.ValueToKey.fields": "service_name,message",
  "header.separator": ";",
  "record.key.path": "$.time",
  "value.converter.date.format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
  "connection.user": "lucx",
  "errors.deadletterqueue.topic.name": "kafka_connect_dead_letter_queue",
  "transforms.Drop.type": "xyz.kafka.connector.transforms.Drop",
  "name": "rest-sink-szc.jms-error-log-to-dingtalk",
  "request.body.format": "json",
  "value.converter.auto.register.schemas": "false",
  "transforms.BloomFilter.bloom.filter.error.rate": "0.002",
  "errors.tolerance": "none",
  "request.body.json.template": "/opt/kafka-connector/dingtalk-templates/jms-error-log.json",
  "oauth2.client.scope": "read_write",
  "value.converter.use.latest.version": "true",
  "transforms.BloomFilter.bloom.filter.capacity": "1000000",
  "oauth2.token.url": "https://sso.pistonint.com/auth/oauth/token",
  "oauth2.client.id": "pistonint_cloud",
  "connection.password": "******",
  "tasks.max": "4",
  "key.converter.latest.compatibility.strict": "false",
  "key.converter.use.latest.version": "true",
  "rest.request.timeout.ms": "1000",
  "consumer.override.max.poll.records": "2000",
  "rest.connect.timeout.ms": "1000",
  "value.converter": "xyz.kafka.connector.convert.json.JsonConverter",
  "headers": "Content-Type:application/json",
  "key.converter.json.fail.invalid.schema": "false",
  "consumer.override.max.request.size": "41943040",
  "topics": "jms-log",
  "batch.size": "1",
  "request.method": "post",
  "transforms.BloomFilter.type": "xyz.kafka.connector.transforms.BloomFilter",
  "transforms.BloomFilter.bloom.filter.expire.seconds": "86400",
  "auth.expires_in_seconds": "3600",
  "transforms.Drop.null.handling.mode": "drop",
  "consumer.override.auto.offset.reset": "latest",
  "transforms.BloomFilter.bloom.filter.key": "******",
  "value.converter.cache.schema": "false"
}
```

### xyz.kafka.connect.rest.sink.RestSinkConnector和xyz.kafka.connect.rest.source.RestSourceConnector公共详细配置

##### `connection.user`

The username to be used with an endpoint requiring authentication.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `connection.password`

The password to be used with an endpoint requiring authentication.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `connection.url`

REST API URL. HTTP URL or HTTPS URL

*Importance:* High

*Type:* String

*Default Value:*

*Validator:* com.pistonint.kafka.connect.validator.UriValidator

##### `batch.size`

The number of records to process as a batch when writing to rest api or reading from rest api.

*Importance:* High

*Type:* Integer

*Default Value:* 2000

*Validator:*

##### `linger.ms`

The number of records to process as a batch when writing to rest api or reading from rest api.

*Importance:* High

*Type:* Integer

*Default Value:* 2000

*Validator:*

##### `behavior.on.null.values`

How to handle records with a non-null key and a null value (i.e. Kafka tombstone records).
Valid options are ``ignore``, ``delete``, ``log`` and ``fail``.
node: When the current connector is RestSinkConnector and behaviorOnNullValues is delete, a delete request will be sent
eg:

```http request
DELETE {{delete_endpoint}}
Authorization: {{authorization}}
Content-Type: application/json

{
  "keys": ["k1", "k2", "k3"]
}
```

*Importance:* High

*Type:* String

*Default Value:* ignore

*Validator:* Matches: ``ignore``, ``delete``, ``log`` , ``fail``

##### `behavior.on.error`

Error handling behavior setting for handling response from HTTP requests. Must be configured to one of the following:
``fail``
Stops the connector when an error occurs.
``ignore``
Continues to process next set of records. when error occurs.
``log``
Logs the error message when error occurs and continues to process next set of records.

*Importance:* High

*Type:* String

*Default Value:* fail

*Validator:* Matches: ``ignore``, ``fail``, ``log``

##### `headers`

REST headers to be included in all requests. Individual headers should be separated by the ``header.separator``
eg: Content-Type:application/json``header.separator``Authorization:xxxxx

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `rest.request.content.type`

HTTP content type

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:* none empty string.

##### `header.separator`

Separator character used in headers property.

*Importance:* Medium

*Type:* String

*Default Value:* ;

*Validator:*

##### `max.retries`

The maximum number of times to retry on errors before failing the task.

*Importance:* Medium

*Type:* Integer

*Default Value:* 5

*Validator:*

##### `retry.backoff.ms`

The time in milliseconds to wait following an error before a retry attempt is made.

*Importance:* Medium

*Type:* Long

*Default Value:* 3000

*Validator:*

##### `retry.codes`

When a rest api request fails, determine whether to retry based on the configured status code.

*Importance:* Medium

*Type:* List

*Default Value:*

*Validator:*

##### `batch.json.as.array`

Send individual messages in a JSON array.

*Importance:* Medium

*Type:* Boolean

*Default Value:* true

*Validator:*

##### `batch.prefix`

Prefix added to record batches. This is applied once at the beginning of the batch of records.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `batch.suffix`

Suffix added to record batches. This is applied once at the end of the batch of records.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `batch.separator`

Separator for records in a batch.

*Importance:* Medium

*Type:* String

*Default Value:* ,

*Validator:*

##### `rest.proxy.host`

The host or ip of the http proxy.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `rest.proxy.port`

The port number of the http proxy.

*Importance:* Medium

*Type:* Integer

*Default Value:*

*Validator:*

##### `rest.proxy.user`

The username to be used when authenticating with the http proxy.

*Importance:* Medium

*Type:* String

*Default Value:*

##### `rest.proxy.password`

The password to be used when authenticating with the http proxy.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `rest.connect.timeout.ms`

Time to wait for a connection to be established.

*Importance:* Medium

*Type:* Long

*Default Value:* 5000

*Validator:*

##### `rest.request.timeout.ms`

Time to wait for a request response to arrive.

*Importance:* Medium

*Type:* Long

*Default Value:* 5000

*Validator:*

##### `auth.type`

Authentication type of the endpoint. Valid values are ``NONE``, ``BASIC``, ``OAUTH2``(Client Credentials grant type
only), ``THIRD_PARTY`` .

*Importance:* Medium

*Type:* String

*Default Value:* NONE

*Validator:*

##### `auth.expires_in_seconds`

Authentication expires in seconds.

*Importance:* Medium

*Type:* Long

*Default Value:* 3600

*Validator:*

##### `oauth2.token.url`

The URL to be used for fetching OAuth2 token. Client Credentials is the only supported grant type.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `oauth2.client.id`

The client id used when fetching OAuth2 token

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `oauth2.client.scope`

The scope used when fetching OAuth2 token

*Importance:* Medium

*Type:* String

*Default Value:* any

*Validator:*

##### `oauth2.client.secret`

The secret used when fetching OAuth2 token.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `oauth2.token.json.path`

The JSON path of the property containing the OAuth2 token returned by the http proxy. Default value is ``access_token``.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `oauth2.client.auth.mode`

Specifies how to encode ``client_id`` and ``client_secret`` in the OAuth2 authorization request. If set to 'header', the
credentials are encoded as an ``'Authorization: Basic <base-64 encoded client_id:client_secret>'`` HTTP header. If set
to 'url', then ``client_id`` and ``client_secret`` are sent as URL encoded parameters.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `oauth2.client.headers`

HTTP headers to be included in each request to the OAuth2 client endpoint. Individual headers should be separated
by `oauth2.client.header.separator`.

*Importance:* Medium

*Type:* String

*Default Value:* Content-Type:application/x-www-form-urlencoded

*Validator:*

##### `oauth2.client.header.separator`

Separator character used in `oauth2.client.headers` property.

*Importance:* Medium

*Type:* String

*Default Value:* |

*Validator:*

##### `third.party.token.req.headers`

JSON containing JWT claims.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `third.party.token.req.separator`

Third party token request header separator

*Importance:* Medium

*Type:* String

*Default Value:* ;

*Validator:*

##### `third.party.token.req.body`

Third party token request body.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `third.party.token.endpoint`

Third party token endpoint.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `third.party.access.token.pointer`

The JSON path of the property containing the token returned by the http proxy.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `third.party.authorization.header.name`

Third party authorization header name.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `third.party.authorization.header.prefix`

Third party authorization header prefix.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

### xyz.kafka.connect.rest.sink.RestSinkConnector详细配置

##### `rest.request.method`

HTTP Request Method. Valid options are ``PUT``, ``POST``.

*Importance:* Medium

*Type:* String

*Default Value:* POST

*Validator:* Matches: ``PUT`` or ``POST``

##### `rest.record.key.json.path`

The field name and JSON path pair of record key. eg: id=$.id,uuid=$.uuid. default is id=$.id
[JSON path](https://support.smartbear.com/alertsite/docs/monitors/api/endpoint/jsonpath.html)
*Importance:* Medium

*Type:* String

*Default Value:* $.id

*Validator:*

##### `rest.request.body.json.template`

The absolute path of json template file. eg: `{"username": "${username}, "addr": "${addr}"}`

[org.apache.commons.text.StringSubstitutor](https://commons.apache.org/proper/commons-text/apidocs/org/apache/commons/text/StringSubstitutor.html)

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:* xyz.kafka.connector.validator.FilePathValidator.FilePathValidator

##### `rest.request.body.format`

Used to produce request body in either JSON or String format
Valid options are ``STRING``, ``JSON``

*Importance:* Medium

*Type:* String

*Default Value:* JSON

*Validator:* Matches: ``STRING``, ``JSON``

### xyz.kafka.connect.rest.source.RestSourceConnector配置

##### `rest.request.method`

HTTP Request Method. Valid options are ``GET``, ``POST``.

*Importance:* Medium

*Type:* String

*Default Value:* POST

*Validator:* Matches: ``GET``, ``POST``

##### `rest.request.body`

REST Request Body.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `rest.request.params`

REST Request params.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `rest.request.offset.initial`

The initial offset of request. eg:k1=v1,current=12,k2=v2

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `rest.response.record.offset.field`

The name of the field offset.

*Importance:* Medium

*Type:* String

*Default Value:* current

*Validator:*

##### `rest.response.parser`

REST Response Parser.

*Importance:* Medium

*Type:* Class

*Default Value:* xyz.kafka.connect.rest.source.parser.StrategyHttpResponseParser

*Validator:*

##### `rest.response.parser.delegate`

The delegate parser of xyz.kafka.connect.rest.source.parser.StrategyHttpResponseParser.

*Importance:* Medium

*Type:* Class

*Default Value:* xyz.kafka.connect.rest.source.parser.FastJsonRecordParser

*Validator:*

##### `rest.response.parser.strategy`

The response parsing strategy of xyz.kafka.connect.rest.source.parser.StrategyHttpResponseParser.

*Importance:* Medium

*Type:* Class

*Default Value:* xyz.kafka.connect.rest.source.strategy.StatusCodeHttpResponseParseStrategy

*Validator:*

##### `rest.response.strategy.codes.process`

When the response status code is in the list, parse the response.

*Importance:* Medium

*Type:* String

*Default Value:* 200..299

*Validator:*

##### `rest.response.strategy.codes.skip`

When the response status code is in the list, skip parse the response.

*Importance:* Medium

*Type:* String

*Default Value:* 300..399

*Validator:*

### 解析器详细配置

##### `rest.response.record.timestamp.parser.class`

The parser class of response timestamp field.

*Importance:* Medium

*Type:* Class

*Default Value:* xyz.kafka.connect.rest.source.parser.EpochMillisTimestampParser

*Validator:*

##### `rest.response.record.key.json.path`

[JSONPath](https://github.com/alibaba/fastjson/wiki/JSONPath)
Field name and JSON path key-value pair list of record key,used to produce kafka key.
eg:id=$.id,name=$.name

*Importance:* Medium

*Type:* List

*Default Value:* id=$.id

*Validator:* xyz.kafka.connector.validator.NonEmptyListValidator

##### `rest.response.record.offset.json.path`

[JSONPath](https://github.com/alibaba/fastjson/wiki/JSONPath)
Field name and JSON path key-value pair list of record offset, used to produce kafka offset.
eg:offset=$.id,service_name=$.service_name

*Importance:* Medium

*Type:* List

*Default Value:* offset=$.offset

*Validator:* xyz.kafka.connector.validator.NonEmptyListValidator

##### `rest.response.record.timestamp.json.path`

[JSONPath](https://github.com/alibaba/fastjson/wiki/JSONPath)
The JSON path of field timestamp, use to produce record timestamp.

*Importance:* Medium

*Type:* String

*Default Value:* $.timestamp

*Validator:*

##### `rest.response.record.timestamp.field.name`

The name of the field timestamp.

*Importance:* Medium

*Type:* String

*Default Value:* timestamp

*Validator:*  None empty string

#### xyz.kafka.connect.rest.source.parser.DateTimeFormatterTimestampParser配置

> ##### `rest.response.record.timestamp.parser.pattern`
> The date time pattern of the field timestamp.
> *Importance:* Medium
> *Type:* String
> *Default Value:* yyyy-MM-dd'T'HH:mm:ss[.SSS]X
> *Validator:* None empty string
>
> ##### `rest.response.record.timestamp.parser.zone`
> The time zone of the field timestamp.
> *Importance:* Medium
> *Type:* String
> *Default Value:* UTC
> *Validator:*

##### `rest.response.record.json.path`

[JSONPath](https://github.com/alibaba/fastjson/wiki/JSONPath)
The JSON path of record, use to produce record value.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:*

##### `rest.response.record.offset.field`

The name of the field offset.

*Importance:* Medium

*Type:* String

*Default Value:* offset

*Validator:* None empty string

##### `key.schema.subject.name`

The schema registry subject name of the key schema.

*Importance:* Medium

*Type:* String

*Default Value:* id.json

*Validator:* None empty string

##### `value.schema.subject.name`

The schema registry subject name of the value schema.

*Importance:* Medium

*Type:* String

*Default Value:*

*Validator:* None empty string

### rate limiter config

> ##### `rate.limiter.key`
>
> The key of the redis rate limiter
> *Importance:* Medium
> *Type:* String
> *Default Value:* equals `connection.url`
> *Validator:* None empty string