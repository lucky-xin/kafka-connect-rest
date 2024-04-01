package xyz.kafka.connect.rest.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;
import static xyz.kafka.connect.rest.source.RestSourceConnectorConfig.RESPONSE_OFFSET_FIELD_DEFAULT;

/**
 * @author luchaoxin
 */
@SuppressWarnings("unchecked")
public class Offset {

    private static final String KEY_KEY = "key";
    private static final String CURRENT_KEY = RESPONSE_OFFSET_FIELD_DEFAULT;

    private static final String TIMESTAMP_KEY = "timestamp";

    private final Map<String, ?> properties;

    private Offset(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static Offset of(Map<String, ?> properties) {
        return new Offset(properties);
    }

    public static Offset of(Map<String, ?> properties, Map<String, ?> key) {
        Map<String, Object> props = new HashMap<>(properties);
        props.put(KEY_KEY, key);
        return new Offset(props);
    }

    public static Offset of(Map<String, ?> properties, Map<String, ?> key, Instant timestamp, int current) {
        Map<String, Object> props = new HashMap<>(properties);
        props.put(KEY_KEY, key);
        props.put(CURRENT_KEY, current);
        props.put(TIMESTAMP_KEY, timestamp.toEpochMilli());
        return new Offset(props);
    }

    public Map<String, ?> toMap() {
        return properties;
    }

    public Optional<Map<String, ?>> getKey() {
        return ofNullable((Map<String, ?>) properties.get(KEY_KEY));
    }

    public Optional<Instant> getTimestamp() {
        return ofNullable((String) properties.get(TIMESTAMP_KEY)).map(Instant::parse);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Offset offset = (Offset) o;
        return Objects.equals(properties, offset.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties);
    }
}
