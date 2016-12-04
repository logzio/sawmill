package io.logz.sawmill;

import io.logz.sawmill.exceptions.ProcessorExecutionException;

import java.util.Optional;

public class ProcessResult {
    private final boolean succeeded;
    private final Optional<Error> error;

    private static ProcessResult processSucceeded = new ProcessResult();

    private ProcessResult() {
        this.succeeded = true;
        this.error = Optional.empty();
    }

    private ProcessResult(String errorMessage) {
        this(errorMessage, null);
    }

    private ProcessResult(String errorMessage, ProcessorExecutionException e) {
        this.succeeded = false;
        this.error = Optional.of(new Error(errorMessage, Optional.ofNullable(e)));
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public Optional<Error> getError() {
        return error;
    }

    public static ProcessResult success() {
        return processSucceeded;
    }

    public static ProcessResult failure(String errorMessage) {
        return new ProcessResult(errorMessage);
    }

    public static ProcessResult failure(String errorMessage, ProcessorExecutionException e) {
        return new ProcessResult(errorMessage, e);
    }

    public static class Error {
        private final String message;
        private final Optional<ProcessorExecutionException> exception;

        public Error(String message, Optional<ProcessorExecutionException> exception) {
            this.message = message;
            this.exception = exception;
        }

        public String getMessage() {
            return message;
        }

        public Optional<ProcessorExecutionException> getException() {
            return exception;
        }
    }
}
