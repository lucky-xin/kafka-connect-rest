package xyz.kafka.connect.rest;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.junit.jupiter.api.Test;
import xyz.kafka.connector.convert.json.JsonConverter;
import xyz.kafka.connector.transforms.Drop;
import xyz.kafka.serialization.json.JsonDataConfig;
import xyz.kafka.utils.StringUtil;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-08-17
 */
public class GroovyScriptTest {

    @Test
    public void test() throws Exception {
        String jsonText = """
                {
                 	"time": "2023-08-23T10:27:04.952058758+08:00",
                 	"thread": "NettyClientWorker-9-9",
                 	"@timestamp": "2023-08-23T10:55:04.952058758+08:00",
                 	"container_image": "/it-job-worker:1.0.2.35-42366269-143",
                 	"service_name": "cz-lshauto",
                 	"level": "ERROR",
                 	"message": "参数绑定错误,请求uri[/evaluate],evaluateReq.mileage binding failure. msg:里程不能超过50w公里;",
                 	"thrown": {
                 		"message": "Connection refused: it-upms/10.28.56.39:20880",
                 		"stack_trace": "io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: it-upms/10.28.56.39:20880\\nCaused by: java.net.ConnectException: Connection refused\\n\\tat java.base/sun.nio.ch.Net.pollConnect(Native Method)\\n\\tat java.base/sun.nio.ch.Net.pollConnectNow(Unknown Source)\\n\\tat java.base/sun.nio.ch.SocketChannelImpl.finishConnect(Unknown Source)\\n\\tat io.netty.channel.socket.nio.NioSocketChannel.doFinishConnect(NioSocketChannel.java:337)\\n\\tat io.netty.channel.nio.AbstractNioChannel$AbstractNioUnsafe.finishConnect(AbstractNioChannel.java:334)\\n\\tat io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:776)\\n\\tat io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:724)\\n\\tat io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:650)\\n\\tat io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:562)\\n\\tat io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:997)\\n\\tat io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)\\n\\tat io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)\\n\\tat java.base/java.lang.Thread.run(Unknown Source)\\n",
                 		"name": "io.netty.channel.AbstractChannel.AnnotatedConnectException"
                 	},
                 	"container_name": "it-auth",
                 	"namespace_name": "piston-cloud",
                 	"pod_name": "it-job-worker-f896cd4d4-zt8nm",
                 	"host": "szc-k8s-node-t6",
                 	"thread_id": 15
                 }
                """;
        JsonDataConfig jsonDataConfig = new JsonDataConfig(
                Map.of(
                        JsonDataConfig.DATE_FORMAT_CONFIG, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
                )
        );
        JsonConverter converter = new JsonConverter();
        converter.configure(
                Map.of(
                        ConverterConfig.TYPE_CONFIG, "value"
                )
        );
        SchemaAndValue schemaAndValue = converter.toConnectData("x", jsonText.getBytes(StandardCharsets.UTF_8));
        Drop<SinkRecord> filter = new Drop<>();
        filter.configure(Map.of(
                        "condition", "valueSchema.fields.keySet().containsAll(['container_name','message','level','@timestamp','namespace_name']) && !value.container_name.startsWith('it-ucar-data') && (currTimeMills - value.'@timestamp'.getTime() < 300000) && value.level == 'ERROR' && value.namespace_name == 'piston-cloud'",
                        "null.handling.mode", "drop"
                )
        );

        SinkRecord sr = new SinkRecord(
                "1",
                1,
                Schema.STRING_SCHEMA,
                "1",
                schemaAndValue.schema(),
                schemaAndValue.value(),
                0
        );
        Schema schema = schemaAndValue.schema();
        Struct struct = (Struct) schemaAndValue.value();

        String[] copyFields = new String[]{"service_name", "message"};
        SchemaBuilder builder = SchemaBuilder.struct();
        for (String copyField : copyFields) {
            builder.field(copyField, schema.field(copyField).schema());
        }

        Struct copyStruct = new Struct(builder.build());
        for (String copyField : copyFields) {
            copyStruct.put(copyField, struct.get(copyField));
        }

        String string = copyStruct.toString();
        String md5 = StringUtil.md5(string);

        SinkRecord sinkRecord = filter.apply(sr);
        System.out.println(sinkRecord);

    }
}
