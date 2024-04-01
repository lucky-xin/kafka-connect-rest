package xyz.kafka.connect.rest.enums;

/**
 * AuthType
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-19
 */
public enum AuthType {
    /**
     * none
     */
    NONE,
    BASIC,
    OAUTH2,
    THIRD_PARTY;
}
