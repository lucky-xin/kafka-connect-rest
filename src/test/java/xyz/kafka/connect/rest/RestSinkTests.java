package xyz.kafka.connect.rest;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import xyz.kafka.connect.rest.sink.RestSinkTask;
import xyz.kafka.connector.convert.json.JsonConverter;
import xyz.kafka.connector.utils.StructUtil;
import xyz.kafka.serialization.json.JsonDataConfig;
import xyz.kafka.utils.StringUtil;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
public class RestSinkTests {

    public KafkaConsumer<byte[], byte[]> consumer() {
        //设置sasl文件的路径
        Properties props = new Properties();
        //设置接入点，请通过控制台获取对应Topic的接入点
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        //设置SSL根证书的路径，请记得将XXX修改为自己的路径
        //与sasl路径类似，该文件也不能被打包到jar中
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/mix.4096.client.truststore.jks");
        //根证书store的密码，保持不变
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
        //接入协议，目前支持使用SASL_SSL协议接入
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");

        // 设置SASL账号
        String saslMechanism = "PLAIN";
        String username = "";
        String password = "";
        String prefix = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", prefix, username, password);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        //SASL鉴权方式，保持不变
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        //Kafka消息的序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        //设置客户端内部重试间隔
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30 * 10000);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lcx-local");
        //hostname校验改成空
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        return new KafkaConsumer<>(props);
    }

    @Test
    public void logicalValueConverterTest() {
        TimeZone UTC = TimeZone.getTimeZone("UTC");
        ZoneId defaultZoneId = ZoneId.of("UTC");
        LocalDate localDate = LocalDate.now();
        Date date = Date.from(localDate.atStartOfDay(defaultZoneId).toInstant());
        Calendar calendar = Calendar.getInstance(UTC);
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        int a = org.apache.kafka.connect.data.Date.fromLogical(
                org.apache.kafka.connect.data.Date.SCHEMA,
                calendar.getTime());
        long epochDay = localDate.toEpochDay();
        System.out.println(epochDay);
    }
}
