package io.logz.sawmill;

import io.logz.sawmill.exceptions.ProcessorExecutionException;

import java.util.Optional;

import static io.logz.sawmill.Result.DROPPED;
import static io.logz.sawmill.Result.FAILED;
import static io.logz.sawmill.Result.SUCCEEDED;

public class ProcessResult {
    private final Result result;
    private final Optional<Error> error;

    private static ProcessResult processSucceeded = new ProcessResult();
    private static ProcessResult processDropped = new ProcessResult(DROPPED);

    private ProcessResult() {
        this(SUCCEEDED);
    }

    private ProcessResult(Result result) {
        this.result = result;
        this.error = Optional.empty();
    }

    private ProcessResult(String errorMessage) {
        this(errorMessage, null);
    }

    private ProcessResult(String errorMessage, ProcessorExecutionException e) {
        this.result = FAILED;
        this.error = Optional.of(new Error(errorMessage, Optional.ofNullable(e)));
    }

    public boolean isSucceeded() {
        return result == SUCCEEDED;
    }

    public boolean isDropped() {
        return result == DROPPED;
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

    public static ProcessResult drop() {
        return processDropped;
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
