package xyz.kafka.connect.rest.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * NullRecordException
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class NullRecordException extends ConnectException {
    public NullRecordException(String message) {
        super(message);
    }
}
