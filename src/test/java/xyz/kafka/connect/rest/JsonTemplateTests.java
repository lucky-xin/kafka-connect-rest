package xyz.kafka.connect.rest;

import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

/**
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
public class JsonTemplateTests {

    @Test
    public void test() {
        String template = """
                {
                  "java_home":"${env:JAVA_HOME}",
                  "secret": "${secret}",
                  "token": "${token}",
                  "msgtype": 2,
                  "markdownMsg": {
                    "markdown": {
                      "title": "错误日志告警",
                      "text": "- 服务名称：${service_name}\\n- 时间：${@timestamp}\\n- 请求Id:${req_id}\\n- 错误消息：${message}\\n- [详情](${kibana_discover_url})}",
                    "at": {
                      "isAtAll": true
                    }
                  }
                }
                """;

        Map<String, Object> data = Map.of(
                "@timestamp", "2023",
                "service_name", "api",
                "req_id", UUID.randomUUID().toString(),
                "message", "",
                "secret", "",
                "token", ""
        );

        StringSubstitutor sub = new StringSubstitutor(data);
        String result = sub.replace(template);
        System.out.println(result);
    }
}
