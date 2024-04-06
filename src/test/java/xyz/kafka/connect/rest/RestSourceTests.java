package xyz.kafka.connect.rest;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.kafka.connect.source.SourceRecord;
import xyz.kafka.connect.rest.source.RestSourceTask;

import java.util.List;
import java.util.Map;

/**
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
public class RestSourceTests {

    /**
     * KAFKA_SCHEMA_REGISTRY_SVC_ENDPOINT=https://x-schema-registry.x-k8s.piston.ink
     * SCHEMA_REGISTRY_CLIENT_REST_HEADERS=Authorization: Basic a2Fma2Euc3I6YTJGbWEyRXVjMk5vWlcxaExuSmxaMmx6ZEhKNUNn
     * REDIS_NODES=gzv-dev-redis-1.piston.ink:6379
     * REDIS_PWD=test.redis.123444
     * REDIS_CLIENT_NAME=lcx
     */
//    @Test
    public void test() {
        String configText = """
                {
                  "connector.class": "xyz.kafka.connect.rest.source.RestSourceConnector",
                  "connection.url": "https://d-it-api.gzv-k8s.piston.ink/model/find",
                  "connection.user": "pistonint.cloud",
                  "connection.password": "pi!s@t%23o2.0!21.ni*nt!1202@",
                  "schema.registry.ssl.endpoint.identification.algorithm":"",
                  "key.converter.json.fail.invalid.schema": "false",
                  "key.converter.latest.compatibility.strict": "false",
                  "key.converter.use.latest.version": "true",
                  "key.converter": "xyz.kafka.connector.convert.json.JsonSchemaConverter",
                  "value.converter": "xyz.kafka.connector.convert.json.JsonSchemaConverter",
                  "value.converter.auto.register.schemas": "false",
                  "value.converter.json.fail.invalid.schema": "false",
                  "value.converter.latest.compatibility.strict": "false",
                  "value.converter.use.latest.version": "true",
                  "value.projection.type": "none",
                  "name": "rest-source.target_data",
                  "producer.override.batch.size": "33554432",
                  "producer.override.compression.type": "zstd",
                  "producer.override.linger.ms": "5",
                  "producer.override.max.request.size": "4194304",
                  "producer.override.message.max.bytes": "33554432",
                  "consumer.override.max.request.size": "4194304",
                  "emit.checkpoints.interval.seconds": "3",
                  "emit.heartbeats.interval.seconds": "3",
                  "consumer.override.max.poll.records": "2000",
                  "errors.deadletterqueue.context.headers.enable": "true",
                  "errors.deadletterqueue.topic.name": "kafka_connect_dead_letter_queue",
                  "errors.deadletterqueue.topic.replication.factor": "8",
                  "errors.log.enable": "true",
                  "errors.log.include.messages": "true",
                  "errors.retry.timeout": "600000",
                  "errors.tolerance": "none",
                  "tasks.max": "4",
                  "exactly.once.support": "required",
                  "rest.request.method": "POST",
                  "rest.request.body": "{\\"brandIds\\":[\\"bra00200\\"]}",
                  "rest.response.parser":"xyz.kafka.connect.rest.source.parser.StrategyHttpResponseParser",
                  "rest.response.parser.delegate": "xyz.kafka.connect.rest.source.parser.FastJsonRecordParser",
                  "rest.response.parser.strategy": "xyz.kafka.connect.rest.source.strategy.StatusCodeHttpResponseParseStrategy",
                  "rest.record.offset.json.path": "id=$.id",
                  "rest.response.record.key.json.path": "id=$.id,name=$.name"
                  "rest.response.record.json.path": "$.data",
                  "key.schema.subject.name":"id.json",
                  "behavior.on.null.values": "log",
                  "behavior.on.error": "log",
                  "rest.request.content.type": "application/json",
                  "max.retries": 5,
                  "retry.backoff.ms": 3000,
                  "auth.type": "OAUTH2",
                  "auth.expires_in_seconds": 3600,
                  "oauth2.token.url": "https://d-it-auth.gzv-k8s.piston.ink/oauth/token?scope=read_write&grant_type=password",
                  "oauth2.client.id": "pistonint_cloud",
                  "oauth2.client.secret": "pi.s#t!on*#cl@oud!@#2021.v5",
                  "oauth2.client.scope": "read_write",
                  "oauth2.token.json.path": "$.access_token",
                  "oauth2.client.auth.mode": "header",
                  "topics": "test.rest"
                }
                """;

        RestSourceTask task = new RestSourceTask();
        Map<String, String> configs = JSON.parseObject(configText, new TypeReference<>() {
        });
        task.start(configs);
        List<SourceRecord> poll = task.poll();
        System.out.println(poll.size());
    }
}
