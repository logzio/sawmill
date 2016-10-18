package io.logz.sawmill.exceptions;

public class ProcessorMissingException extends RuntimeException {
    public ProcessorMissingException(String errorMsg) {
        super(errorMsg);
    }
}
