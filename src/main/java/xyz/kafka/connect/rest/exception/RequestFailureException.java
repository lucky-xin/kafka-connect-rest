package xyz.kafka.connect.rest.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * RequestFailureException
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class RequestFailureException extends ConnectException {
    private final Integer status;
    private final String reasonPhrase;
    private final String payloadString;
    private final String formattedUrl;
    private final String responseContent;
    private final Exception exception;
    private final String msg;

    public RequestFailureException(
            Integer status,

            String reasonPhrase,
            String payloadString,
            String formattedUrl,
            String responseContent,
            Exception exception,
            String message) {
        super(String.format("REST Response code: %d, Reason phrase: %s, Url: %s, Exception: %s, Error message: %s",
                status, reasonPhrase, formattedUrl, exception, message));
        this.status = status;
        this.reasonPhrase = reasonPhrase;
        this.payloadString = payloadString;
        this.formattedUrl = formattedUrl;
        this.responseContent = responseContent;
        this.exception = exception;
        this.msg = message;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Error while processing REST request with ");
        builder.append("Url : ").append(this.formattedUrl).append(", ");
        if (!(this.status == null || this.reasonPhrase == null)) {
            builder.append("Status code : ").append(this.status).append(", ");
            builder.append("Reason Phrase : ").append(this.reasonPhrase).append(", ");
        }
        if (this.msg != null) {
            builder.append("Error Message : ").append(this.msg).append(", ");
        }
        if (this.exception != null) {
            builder.append("Exception : ").append(this.exception);
        }
        return builder.toString();
    }

    public Exception exception() {
        return this.exception;
    }

    public Integer statusCode() {
        return this.status;
    }

    public String responseContent() {
        return this.responseContent;
    }

    public String reasonPhrase() {
        return this.reasonPhrase;
    }

    public String payload() {
        return this.payloadString;
    }

    public String url() {
        return this.formattedUrl;
    }

    public String errorMessage() {
        return this.msg;
    }
}
