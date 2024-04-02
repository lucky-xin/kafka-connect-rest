package xyz.kafka.connect.rest.enums;

/**
 * BehaviorOnNullValues
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-19
 */
public enum BehaviorOnNullValues {
    /**
     * ignore
     */
    IGNORE,
    DELETE,
    LOG,
    FAIL;
}
