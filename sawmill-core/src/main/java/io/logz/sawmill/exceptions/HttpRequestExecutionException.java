package io.logz.sawmill.exceptions;

public class HttpRequestExecutionException extends SawmillException {
    public HttpRequestExecutionException(String errorMsg) {
        super(errorMsg);
    }

    public HttpRequestExecutionException(String errorMsg, Exception e) {
        super(errorMsg, e);
    }
}
