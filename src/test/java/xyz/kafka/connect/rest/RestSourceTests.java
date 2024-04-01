package xyz.kafka.connect.rest;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import xyz.kafka.connect.rest.source.RestSourceTask;

import java.util.List;
import java.util.Map;

/**
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
public class RestSourceTests {

    @Test
    public void test() {
        String configText = """
                {
                  "connector.class": "xyz.kafka.connect.rest.source.RestSourceConnector",
                  "consumer.override.max.request.size": "41943040",
                  "emit.checkpoints.interval.seconds": "3",
                  "emit.heartbeats.interval.seconds": "3",
                  "key.converter": "xyz.kafka.connector.convert.json.JsonSchemaConverter",
                  "name": "rest-source.target_data",
                  "producer.override.batch.size": "33554432",
                  "producer.override.compression.type": "lz4",
                  "producer.override.linger.ms": "5",
                  "producer.override.max.request.size": "41943040",
                  "producer.override.message.max.bytes": "33554432",
                  "value.converter": "xyz.kafka.connector.convert.json.JsonSchemaConverter",
                  "collection": "valuation_cache",
                  "consumer.override.max.poll.records": "2000",
                  "database": "rv_valuation",
                  "delete.on.null.values": "false",
                  "errors.deadletterqueue.context.headers.enable": "true",
                  "errors.deadletterqueue.topic.name": "kafka_connect_dead_letter_queue",
                  "errors.deadletterqueue.topic.replication.factor": "8",
                  "errors.log.enable": "true",
                  "errors.log.include.messages": "true",
                  "errors.retry.timeout": "600000",
                  "errors.tolerance": "none",
                  "key.converter.json.fail.invalid.schema": "false",
                  "key.converter.latest.compatibility.strict": "false",
                  "key.converter.schema.registry.url": "http://kafka-schema-registry:8080",
                  "key.converter.use.latest.version": "true",
                  "tasks.max": "4",
                  "topics": "rv_valuation.valuation_cache",
                  "value.converter.auto.register.schemas": "false",
                  "value.converter.cache.schema": "false",
                  "value.converter.json.fail.invalid.schema": "false",
                  "value.converter.latest.compatibility.strict": "false",
                  "value.converter.schema.registry.url": "http://kafka-schema-registry:8080",
                  "value.converter.use.latest.version": "true",
                  "value.projection.type": "none",
                  "rest.request.method": "post",
                  "rest.request.body": "{\\"brandIds\\":[\\"bra00200\\"]}",
                  "rest.api.url": "http://127.0.0.1:5555/model/find",
                  "behavior.on.null.values": "log",
                  "behavior.on.error": "log",
                  "headers": "Content-Type: application/json",
                  "auth.type": "OAUTH2",
                  "connection.user": "lucx",
                  "connection.password": "xxxxxxxxxxxx",
                  "oauth2.token.url": "http://127.0.0.1:6666/oauth/token?scope=read_write&grant_type=password",
                  "oauth2.client.id": "",
                  "oauth2.client.secret": "",
                  "oauth2.client.scope": "read_write",
                  "oauth2.token.property": "access_token",
                  "oauth2.client.auth.mode": "header",
                  "rest.response.list.pointer": "/data",
                  "rest.response.record.key.pointer": "/id",
                  "kafka.topic": "test.rest"
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
