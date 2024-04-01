package xyz.kafka.connect.rest.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * ReportingFailureException
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class ReportingFailureException extends ConnectException {
    public ReportingFailureException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
