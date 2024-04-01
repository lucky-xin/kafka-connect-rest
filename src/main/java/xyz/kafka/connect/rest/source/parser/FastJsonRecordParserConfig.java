package xyz.kafka.connect.rest.source.parser;

import com.alibaba.fastjson2.JSONPath;
import com.google.common.base.Splitter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * AbstractJacksonParserConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
public class FastJsonRecordParserConfig extends AbstractConfig {

    private static final String RECORD_TIMESTAMP_PARSER_CLASS = "rest.response.record.timestamp.parser";
    private static final String ITEM_KEY_POINTER = "rest.response.record.key.pointer";
    private static final String ITEM_TIMESTAMP_POINTER = "rest.response.record.timestamp.pointer";
    private static final String ITEM_OFFSET_VALUE_POINTER = "rest.response.record.offset.pointer";
    private static final String KEY_SCHEMA_SUBJECT_NAME = "key.schema.subject.name";
    private static final String VALUE_SCHEMA_SUBJECT_NAME = "value.schema.subject.name";

    private static final String RESPONSE_RECORD_POINTER = "rest.response.record.pointer";
    private static final String RESPONSE_RECORD_OFFSET_FIELD = "rest.response.offset.field";
    private static final String TOPICS = "topics";

    private final TimestampParser timestampParser;
    private final Map<String, JSONPath> keyPointers;
    private final Optional<JSONPath> timestampPointer;
    private final JSONPath responseRecordPointer;
    private final Map<String, JSONPath> offsetPointers;
    private final String topics;

    private final String keySubjectName;
    private final String valueSubjectName;


    protected FastJsonRecordParserConfig(Map<String, ?> originals) {
        super(conf(), originals);
        timestampParser = getConfiguredInstance(RECORD_TIMESTAMP_PARSER_CLASS, TimestampParser.class);
        keyPointers = Optional.ofNullable(getString(ITEM_KEY_POINTER))
                .map(t ->
                        Splitter.on(",")
                                .trimResults()
                                .omitEmptyStrings()
                                .withKeyValueSeparator("=")
                                .split(t)
                                .entrySet()
                ).stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, t -> JSONPath.of(t.getValue())));
        timestampPointer = Optional.ofNullable(getString(ITEM_TIMESTAMP_POINTER)).map(JSONPath::of);

        offsetPointers = Optional.ofNullable(getString(ITEM_OFFSET_VALUE_POINTER))
                .map(t ->
                        Splitter.on(",")
                                .trimResults()
                                .omitEmptyStrings()
                                .withKeyValueSeparator("=")
                                .split(t)
                                .entrySet()
                ).stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, t -> JSONPath.of(t.getValue())));
        topics = getString(TOPICS);
        keySubjectName = getString(KEY_SCHEMA_SUBJECT_NAME);
        valueSubjectName = getString(VALUE_SCHEMA_SUBJECT_NAME);
        responseRecordPointer = Optional.ofNullable(getString(RESPONSE_RECORD_POINTER))
                .map(JSONPath::of)
                .orElseThrow(() -> new ConnectException("invalid response record pointer"));
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(RECORD_TIMESTAMP_PARSER_CLASS, CLASS, EpochMillisOrDelegateTimestampParser.class, LOW,
                        "Record Timestamp parser class")
                .define(ITEM_KEY_POINTER, STRING, null, HIGH, "Item Key JsonPointers")
                .define(ITEM_TIMESTAMP_POINTER, STRING, null, MEDIUM, "Item Timestamp JsonPointer")
                .define(ITEM_OFFSET_VALUE_POINTER, STRING, "", MEDIUM, "Item Offset JsonPointers")
                .define(TOPICS, STRING, HIGH, "Kafka Topics")
                .define(KEY_SCHEMA_SUBJECT_NAME, STRING, null,
                        ConfigDef.Importance.MEDIUM, "The subject of key schema")
                .define(VALUE_SCHEMA_SUBJECT_NAME, ConfigDef.Type.CLASS, null,
                        ConfigDef.Importance.MEDIUM, "The subject of value schema")
                .define(RESPONSE_RECORD_POINTER, STRING, null,
                        ConfigDef.Importance.MEDIUM, "Response record point")
                .define(RESPONSE_RECORD_OFFSET_FIELD, STRING, null,
                        ConfigDef.Importance.MEDIUM, "Response offset field");
    }

    public TimestampParser timestampParser() {
        return timestampParser;
    }

    public JSONPath responseRecordPointer() {
        return responseRecordPointer;
    }

    public Map<String, JSONPath> keyPointers() {
        return keyPointers;
    }

    public Optional<JSONPath> timestampPointer() {
        return timestampPointer;
    }

    public Map<String, JSONPath> offsetPointers() {
        return offsetPointers;
    }

    public String keySubjectName() {
        return keySubjectName;
    }

    public String valueSubjectName() {
        return valueSubjectName;
    }

    public String topic() {
        return topics;
    }
}
