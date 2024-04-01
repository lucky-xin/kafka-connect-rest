package xyz.kafka.connect.rest.auth;

import java.util.Arrays;
import java.util.List;

/**
 * SignatureAlgorithm
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public enum SignatureAlgorithm {
    /**
     *
     */
    RS256;

    public static List<String> getValidValues() {
        return Arrays.stream(values()).map(Enum::name).toList();
    }
}
