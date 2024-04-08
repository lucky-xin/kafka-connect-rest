package xyz.kafka.connect.rest.source.parser;

import com.alibaba.fastjson2.JSONPath;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import xyz.kafka.connector.validator.Validators;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * FastJsonRecordParserConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-21
 */
public class FastJsonRecordParserConfig extends AbstractConfig {
    private static final String RECORD_KEY_JSON_PATH = "rest.response.record.key.json.path";

    private static final String RECORD_TIMESTAMP_PARSER_CLASS = "rest.response.record.timestamp.parser.class";
    private static final String RECORD_TIMESTAMP_JSON_PATH = "rest.response.record.timestamp.json.path";
    private static final String RECORD_JSON_PATH = "rest.response.record.json.path";
    private static final String RECORD_TIMESTAMP_FIELD_NAME = "rest.response.record.timestamp.field.name";
    public static final String RECORD_TIMESTAMP_FIELD_NAME_DEFAULT = "timestamp";
    private static final String RECORD_TIMESTAMP_FIELD_NAME_DOC = "Record key field name,eg: id,uuid,default is id";
    private static final String RECORD_TIMESTAMP_FIELD_NAME_DISPLAY = RECORD_TIMESTAMP_FIELD_NAME_DOC;
    private static final String RECORD_OFFSET_JSON_PATH = "rest.record.offset.json.path";
    private static final String RECORD_OFFSET_FIELD = "rest.response.record.offset.field";
    private static final String KEY_SCHEMA_SUBJECT_NAME = "key.schema.subject.name";
    private static final String VALUE_SCHEMA_SUBJECT_NAME = "value.schema.subject.name";
    private static final String TOPICS = "topics";
    private final Map<String, JSONPath> keyJsonPaths;
    private final Map<String, JSONPath> offsetJsonPaths;
    private final Optional<JSONPath> timestampJsonPath;
    private final JSONPath recordJsonPath;
    private final TimestampParser timestampParser;
    private final String topics;
    private final String timestampFieldName;
    private final Optional<String> offsetFieldName;

    private final String keySubjectName;
    private final String valueSubjectName;

    protected FastJsonRecordParserConfig(Map<String, ?> originals) {
        super(conf(), originals);
        timestampParser = getConfiguredInstance(RECORD_TIMESTAMP_PARSER_CLASS, TimestampParser.class);
        keyJsonPaths = getList(RECORD_KEY_JSON_PATH)
                .stream()
                .map(t -> t.split("="))
                .collect(Collectors.toMap(
                        t -> t[0].strip(),
                        t -> JSONPath.of(t[1].strip()))
                );
        offsetJsonPaths = getList(RECORD_OFFSET_JSON_PATH)
                .stream()
                .map(t -> t.split("="))
                .collect(Collectors.toMap(
                                t -> t[0].strip(),
                                t -> JSONPath.of(t[1].strip())
                        )
                );
        timestampJsonPath = Optional.ofNullable(getString(RECORD_TIMESTAMP_JSON_PATH)).map(JSONPath::of);
        topics = getString(TOPICS);
        timestampFieldName = getString(RECORD_TIMESTAMP_FIELD_NAME);
        offsetFieldName = Optional.ofNullable(getString(RECORD_OFFSET_FIELD));
        keySubjectName = getString(KEY_SCHEMA_SUBJECT_NAME);
        valueSubjectName = getString(VALUE_SCHEMA_SUBJECT_NAME);
        recordJsonPath = Optional.ofNullable(getString(RECORD_JSON_PATH))
                .map(JSONPath::of)
                .orElseThrow(() -> new ConnectException("invalid response record pointer"));
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(
                        RECORD_TIMESTAMP_PARSER_CLASS,
                        CLASS,
                        EpochMillisTimestampParser.class.getName(),
                        LOW,
                        "The parser class of response timestamp field.")
                .define(
                        RECORD_KEY_JSON_PATH,
                        LIST,
                        "id=$.id",
                        Validators.nonEmptyList(),
                        HIGH,
                        "Field name and JSON path key-value pair list of record key,used to produce kafka key. eg:id=$.id,name=$.name")
                .define(
                        RECORD_TIMESTAMP_JSON_PATH,
                        STRING,
                        "$.timestamp",
                        MEDIUM,
                        "The JSON path of timestamp, used to produce record timestamp."
                ).define(
                        RECORD_TIMESTAMP_FIELD_NAME,
                        STRING,
                        RECORD_TIMESTAMP_FIELD_NAME_DEFAULT,
                        Validators.nonEmptyString(),
                        MEDIUM,
                        RECORD_TIMESTAMP_FIELD_NAME_DISPLAY
                ).define(
                        RECORD_OFFSET_JSON_PATH,
                        LIST,
                        "offset=$.offset",
                        Validators.nonEmptyList(),
                        MEDIUM,
                        "Field name and JSON path key-value pair list of record offset, used to produce kafka offset. eg:offset=$.offset,service_name=$.service_name"
                ).define(
                        TOPICS,
                        STRING,
                        HIGH,
                        "Kafka Topics")
                .define(
                        KEY_SCHEMA_SUBJECT_NAME,
                        STRING,
                        "id.json",
                        ConfigDef.Importance.MEDIUM,
                        "The subject of key schema"
                ).define(
                        VALUE_SCHEMA_SUBJECT_NAME,
                        STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "The subject of value schema")
                .define(
                        RECORD_JSON_PATH,
                        STRING,
                        "$.data",
                        Validators.nonEmptyString(),
                        ConfigDef.Importance.MEDIUM,
                        "The JSON path of record, use to produce record value."
                ).define(
                        RECORD_OFFSET_FIELD,
                        STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "The name of the field offset in the record"
                );
    }

    public TimestampParser timestampParser() {
        return timestampParser;
    }

    public JSONPath recordPointer() {
        return recordJsonPath;
    }

    public Map<String, JSONPath> keyJsonPaths() {
        return keyJsonPaths;
    }

    public Optional<JSONPath> timestampJsonPath() {
        return timestampJsonPath;
    }

    public Map<String, JSONPath> offsetJsonPaths() {
        return offsetJsonPaths;
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

    public String timestampFieldName() {
        return timestampFieldName;
    }

    public Optional<String> offsetFieldName() {
        return offsetFieldName;
    }
}
