package xyz.kafka.connect.rest.source.parser;
/*
 *            Copyright © 2024 chaoxin.lu
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.schema.JSONSchema;
import com.alibaba.fastjson2.schema.ValidateResult;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.saasquatch.jsonschemainferrer.FormatInferrers;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import xyz.kafka.connect.rest.source.RestSourceConnectorConfig;
import xyz.kafka.connector.utils.StructUtil;
import xyz.kafka.registry.client.CachedSchemaRegistryCli;
import xyz.kafka.schema.generator.JsonSchemaGenerator;
import xyz.kafka.serialization.json.JsonData;
import xyz.kafka.utils.ConfigUtil;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

/**
 * FastJsonRecordParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class FastJsonRecordParser implements HttpResponseParser {

    private JsonData jsonData;
    private JsonSchemaGenerator jsonSchemaGenerator;

    private FastJsonRecordParserConfig config;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private CachedSchemaRegistryCli schemaRegistry;

    private final Cache<String, Schema> cache = Caffeine.newBuilder()
            .expireAfterWrite(6, TimeUnit.HOURS)
            .maximumSize(1000)
            .softValues()
            .build();

    private final AtomicInteger localOffset = new AtomicInteger(0);

    private RestSourceConnectorConfig c;

    @Override
    public void configure(Map<String, ?> map, AbstractConfig config) {
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false);
        OBJECT_MAPPER.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        OBJECT_MAPPER.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, false);
        this.jsonData = new JsonData();
        this.jsonSchemaGenerator = new JsonSchemaGenerator(false, true, true,
                List.of(
                        FormatInferrers.ip(),
                        FormatInferrers.email(),
                        FormatInferrers.dateTime()
                )
        );
        this.config = new FastJsonRecordParserConfig(map);
        this.schemaRegistry = new CachedSchemaRegistryCli(
                List.of(System.getenv("KAFKA_SCHEMA_REGISTRY_SVC_ENDPOINT")),
                1000,
                Collections.singletonList(new JsonSchemaProvider()),
                config.originalsWithPrefix("schema.registry."),
                ConfigUtil.getRestHeaders("SCHEMA_REGISTRY_CLIENT_REST_HEADERS")
        );
        if (config instanceof RestSourceConnectorConfig x) {
            c = x;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SourceRecord> parse(ClassicHttpResponse resp) {
        try {
            JSONObject res = JSON.parseObject(resp.getEntity().getContent().readAllBytes());
            Object data = config.recordPointer().eval(res);
            if (data instanceof Collection<?> coll) {
                return coll.stream()
                        .map(t -> (Map<String, Object>) t)
                        .map(this::toSourceRecord)
                        .toList();
            }
            if (data instanceof Map<?, ?> m) {
                return Stream.of(m)
                        .map(t -> (Map<String, Object>) t)
                        .map(this::toSourceRecord)
                        .toList();
            }
            String clazz = data == null ? null : data.getClass().getName();
            throw new UnsupportedOperationException("Unsupported response type:" + clazz);
        } catch (IOException | UnsupportedOperationException e) {
            throw new ConnectException(e);
        }

    }

    protected final SourceRecord toSourceRecord(Map<String, Object> data) {
        Map<String, Object> offset = getOffset(data);
        Instant instant = getTimestamp(data)
                .map(t -> config.timestampParser().parse(t))
                .orElseGet(() -> ofNullable(offset.get(this.config.timestampFieldName()))
                        .map(String.class::cast)
                        .map(t -> config.timestampParser().parse(t))
                        .orElse(Instant.now())
                );
        Map<String, Object> key = getKey(data);
        Struct keyStruct = toStruct(key, config.keySubjectName(), true);
        Struct valueStruct = toStruct(data, config.valueSubjectName(), false);
        Map<String, ?> sourcePartition = Collections.emptyMap();
        int current = config.offsetFieldName()
                .map(t -> MapUtil.getInt(data, t))
                .orElse(this.localOffset.incrementAndGet());
        Map<String, Object> sourceOffset = new HashMap<>(offset);

        sourceOffset.put(FastJsonRecordParserConfig.KEY_KEY, key);
        sourceOffset.put(FastJsonRecordParserConfig.CURRENT_KEY, current);
        sourceOffset.put(FastJsonRecordParserConfig.TIMESTAMP_KEY, offset);
        c.offsetTracker().pendingRecord(sourceOffset);
        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                config.topic(),
                null,
                keyStruct.schema(),
                keyStruct,
                valueStruct.schema(),
                valueStruct,
                instant.toEpochMilli()
        );
    }

    private Struct toStruct(Map<String, Object> data, String subjectName, boolean key) {
        try {
            Schema schema;
            if (subjectName != null) {
                SchemaMetadata meta = schemaRegistry.getLatestSchemaMetadata(subjectName);
                String schemaText = meta.getSchema();
                JSONSchema jsonSchema = JSONSchema.of(JSON.parseObject(schemaText));
                ValidateResult result = jsonSchema.validate(data);
                if (!result.isSuccess()) {
                    throw new ConnectException(result.getMessage());
                }
                schema = jsonData.toConnectSchema(new JsonSchema(schemaText), Map.of());
            } else {
                schema = cache.get("connect_schema:" + config.topic() + ":" + key, k -> {
                    ObjectNode on = jsonSchemaGenerator.toSchema(OBJECT_MAPPER.valueToTree(data));
                    JsonSchema js = new JsonSchema(on);
                    return jsonData.toConnectSchema(js, Map.of());
                });
            }
            return StructUtil.toConnectData(schema, data);
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    Map<String, Object> getKey(Map<String, Object> node) {
        return config.keyJsonPaths()
                .entrySet()
                .stream()
                .collect(toMap(Entry::getKey, e -> e.getValue().eval(node)));
    }

    Optional<String> getTimestamp(Map<String, Object> node) {
        return config.timestampJsonPath().map(pointer -> pointer.eval(node))
                .map(Object::toString);
    }

    Map<String, Object> getOffset(Map<String, Object> node) {
        return config.offsetJsonPaths()
                .entrySet()
                .stream()
                .collect(toMap(Entry::getKey, e -> e.getValue().eval(node)));
    }
}
