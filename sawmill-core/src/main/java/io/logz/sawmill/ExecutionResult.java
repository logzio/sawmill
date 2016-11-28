package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;

import java.util.Optional;

public class ExecutionResult {
    private final boolean succeeded;
    private final Optional<Error> error;

    private static ExecutionResult executionSucceeded = new ExecutionResult();

    private ExecutionResult() {
        this.succeeded = true;
        this.error = Optional.empty();
    }

    private ExecutionResult(String errorMessage, String failedProcessorName) {
        this(errorMessage, failedProcessorName, null);
    }

    private ExecutionResult(String errorMessage, String failedProcessorName, PipelineExecutionException e) {
        this.succeeded = false;
        this.error = Optional.of(new Error(errorMessage, failedProcessorName, e == null ? Optional.empty() : Optional.of(e)));
    }

    public static ExecutionResult success() {
        return executionSucceeded;
    }

    public static ExecutionResult failure(String errorMessage, String failedProcessorName) {
        return new ExecutionResult(errorMessage, failedProcessorName);
    }

    public static ExecutionResult failure(String errorMessage, String failedProcessorName, PipelineExecutionException e) {
        return new ExecutionResult(errorMessage, failedProcessorName, e);
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public Optional<Error> getError() {
        return error;
    }

    public static class Error {
        private final String message;
        private final String failedProcessorName;
        private final Optional<PipelineExecutionException> exception;

        public Error(String message, String failedProcessorName, Optional<PipelineExecutionException> exception) {
            this.message = message;
            this.failedProcessorName = failedProcessorName;
            this.exception = exception;
        }

        public String getMessage() {
            return message;
        }

        public String getFailedProcessorName() {
            return failedProcessorName;
        }

        public Optional<PipelineExecutionException> getException() {
            return exception;
        }
    }
}
