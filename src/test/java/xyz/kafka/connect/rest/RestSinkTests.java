package xyz.kafka.connect.rest;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.saasquatch.jsonschemainferrer.FormatInferrers;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import xyz.kafka.connect.rest.sink.RestSinkTask;
import xyz.kafka.schema.generator.JsonSchemaGenerator;
import xyz.kafka.serialization.json.JsonData;
import xyz.kafka.utils.StringUtil;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
class RestSinkTests {

    public KafkaConsumer<byte[], byte[]> consumer() {
        //设置sasl文件的路径
        Properties props = new Properties();
        //设置接入点，请通过控制台获取对应Topic的接入点
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "gzv-x-kafk-1.piston.ink:9092,gzv-x-kafk-2.piston.ink:9092");
        //设置SSL根证书的路径，请记得将XXX修改为自己的路径
        //与sasl路径类似，该文件也不能被打包到jar中
//        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/mix.4096.client.truststore.jks");
        //根证书store的密码，保持不变
//        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
        //接入协议，目前支持使用SASL_SSL协议接入
//        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

        // 设置SASL账号
        String saslMechanism = "PLAIN";
        String username = "";
        String password = "";
        String prefix = "org.apache.kafka.common.security.plain.PlainLoginModule";
//        String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", prefix, username, password);
//        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        //SASL鉴权方式，保持不变
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        //Kafka消息的序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        //设置客户端内部重试间隔
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30 * 10000);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lcx-local");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        //hostname校验改成空
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        return new KafkaConsumer<>(props);
    }

//    @Test
    void pollDataTest() throws IOException {
        String configText = """
                {
                  "connector.class": "xyz.kafka.connect.rest.sink.RestSinkConnector",
                  "connection.url": "http://dingtalk-hook.piston-alert.svc.cluster.local:6666/message/markdown",
                  "connection.user": "pistonint.cloud",
                  "connection.password": "pi!s@t%23o2.0!21.ni*nt!1202@",
                  "auth.type": "OAUTH2",
                  "auth.expires_in_seconds": 3600,
                  "oauth2.token.url": "https://d-it-auth.gzv-k8s.piston.ink/oauth/token?scope=read_write&grant_type=password",
                  "oauth2.client.id": "pistonint_cloud",
                  "oauth2.client.secret": "pi.s#t!on*#cl@oud!@#2021.v5",
                  "oauth2.client.scope": "read_write",
                  "oauth2.token.json.path": "$.access_token",
                  "oauth2.client.auth.mode": "header",
                  
                  "tasks.max": "4",
                  "name": "rest-sink-szc.jms-error-log-to-dingtalk",
                  "behavior.on.null.values": "ignore",
                  "behavior.on.error": "log",
                  "rest.request.method": "POST",
                  "rest.request.body.json.template": "/Users/chaoxin.lu/IdeaProjects/piston-kafka-connector-template/config/jms-error-log.json",
                  "rest.request.content.type": "application/json",
                  "rest.request.body.format": "json",
                  "rest.connect.timeout.ms": "1000",
                  "rest.request.timeout.ms": "1000",
                  
                  "batch.json.as.array": "false",
                  "batch.size": "2",
                  
                  "consumer.override.max.request.size": "4194304",
                  "consumer.override.max.poll.records": "2000",
                  "consumer.override.auto.offset.reset": "latest",
                  "topics": "jms-log",
                  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                  "value.converter": "xyz.kafka.connector.convert.json.JsonConverter",
                  "value.converter.schemas.enable": "false",
                  "value.converter.decimal.format": "NUMERIC",
                  "value.converter.use.big.decimal.for.floats": "false",
                  "value.converter.schema.gen.date.time.infer.enabled": "false",
                  "value.converter.write.big.decimal.as.plain": "true",
                  "value.converter.cache.schemas.enabled": "false",
                  "value.converter.auto.register.schemas": "false",
                  "value.schema.subject.name": "log.jms_value_json",
                  
                  "transforms": "ValueToKey,Drop,ToStruct,BloomFilter",
                  "transforms.Drop.type": "xyz.kafka.connector.transforms.Drop",
                  "transforms.Drop.condition": "valueSchema.fields.keySet().containsAll(['container_name','service_name','message','level','@timestamp','namespace_name']) && !value.container_name.startsWith('it-ucar-data') && value.level == 'ERROR' && value.namespace_name == 'piston-cloud'",
                  "transforms.Drop.null.handling.mode": "drop",
                  "transforms.ValueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
                  "transforms.ValueToKey.fields": "time",
                  "transforms.BloomFilter.type": "xyz.kafka.connector.transforms.BloomFilter",
                  "transforms.BloomFilter.bloom.filter.key": "bf:{rest-sink-szc}.jms-error-log-to-dingtalk",
                  "transforms.BloomFilter.bloom.filter.capacity": "1000000",
                  "transforms.BloomFilter.bloom.filter.error.rate": "0.002",
                  "transforms.BloomFilter.bloom.filter.expire.seconds": "3600",
                  "transforms.ToStruct.field.pairs": "biz_resp:biz_resp,req_params:req_params",
                  "transforms.ToStruct.type": "xyz.kafka.connector.transforms.ToStruct$Value",
                  "transforms.ToStruct.behavior.on.error": "LOG",
                  "transforms.StringIdToStruct.type": "xyz.kafka.connector.transforms.StringIdToStruct",
                  
                  "errors.deadletterqueue.context.headers.enable": "true",
                  "errors.deadletterqueue.topic.name": "kafka_connect_dead_letter_queue",
                  "errors.deadletterqueue.topic.replication.factor": "8",
                  "errors.log.enable": "true",
                  "errors.log.include.messages": "true",
                  "errors.retry.timeout": "600000",
                  "errors.tolerance": "none",
                }
                """;

        RestSinkTask task = new RestSinkTask();
        Map<String, String> configs = JSON.parseObject(configText, new TypeReference<>() {
        });
        task.start(configs);
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(false, true, true,
                List.of(
                        FormatInferrers.ip(),
                        FormatInferrers.email(),
                        FormatInferrers.dateTime()
                )
        );
        ObjectMapper mapper = new ObjectMapper();
        JsonData jsonData = new JsonData();
        KafkaConsumer<byte[], byte[]> consumer = consumer();
        consumer.assign(List.of(new TopicPartition("turkey", 0)));
        try (consumer) {
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(10));
                Collection<SinkRecord> sinkRecords = new LinkedList<>();
                Iterator<ConsumerRecord<byte[], byte[]>> itr = records.iterator();
                while (itr.hasNext()) {
                    ConsumerRecord<byte[], byte[]> record = itr.next();
                    String key = StringUtil.getUuid();
                    JsonNode jsonNode = mapper.readTree(record.value());
                    ObjectNode schema = jsonSchemaGenerator.toSchema(jsonNode);
                    JsonSchema js = new JsonSchema(schema);
                    SchemaAndValue sav = jsonData.toConnectData(js, jsonNode);
                    SinkRecord sr = new SinkRecord(
                            record.topic(),
                            record.partition(),
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            key,
                            sav.schema(),
                            sav.value(),
                            record.offset()
                    );
                    sinkRecords.add(sr);
                }
                task.put(sinkRecords);
            }
        }
    }
}
