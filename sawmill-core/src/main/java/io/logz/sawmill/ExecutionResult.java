package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;

import java.util.Optional;

import static io.logz.sawmill.Result.DROPPED;
import static io.logz.sawmill.Result.EXPIRED;
import static io.logz.sawmill.Result.FAILED;
import static io.logz.sawmill.Result.SUCCEEDED;

public class ExecutionResult {
    private final Result result;
    private final Optional<Error> error;
    private Optional<Long> overtimeTook;

    private static ExecutionResult executionSucceeded = new ExecutionResult();
    private static ExecutionResult executionDropped = new ExecutionResult(DROPPED);
    private static ExecutionResult executionExpired = new ExecutionResult(EXPIRED);

    private ExecutionResult() {
        this(SUCCEEDED);
    }

    private ExecutionResult(Result result) {
        this(result, Optional.empty());
    }

    private ExecutionResult(Result result, Optional<Long> overtimeTook) {
        this.result = result;
        this.error = Optional.empty();
        this.overtimeTook = overtimeTook;
    }

    private ExecutionResult(String errorMessage, String failedProcessorName) {
        this(errorMessage, failedProcessorName, null);
    }

    private ExecutionResult(String errorMessage, String failedProcessorName, PipelineExecutionException e) {
        this.result = FAILED;
        this.error = Optional.of(new Error(errorMessage, failedProcessorName, Optional.ofNullable(e)));
        this.overtimeTook = Optional.empty();
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

    public static ExecutionResult overtime(ExecutionResult executionResult, long timeTook) {
        if (executionResult.isFailed()) {
            executionResult.setOvertime(timeTook);
            return executionResult;
        }

        return new ExecutionResult(executionResult.result, Optional.of(timeTook));
    }

    public static ExecutionResult expired() {
        return executionExpired;
    }

    public static ExecutionResult expired(long timeTook) {
        ExecutionResult result = new ExecutionResult(EXPIRED);
        result.setOvertime(timeTook);
        return result;
    }

    public static ExecutionResult dropped() {
        return executionDropped;
    }

    public boolean isSucceeded() {
        return result == SUCCEEDED;
    }
    public boolean isFailed() {
        return result == FAILED;
    }
    public boolean isDropped() {
        return result == DROPPED;
    }
    public boolean isOvertime() {
        return overtimeTook.isPresent();
    }
    public boolean isExpired() {
        return result == EXPIRED;
    }

    public Optional<Error> getError() {
        return error;
    }

    public Optional<Long> getOvertimeTook() {
        return overtimeTook;
    }

    public void setOvertime(long timeTook) {
        this.overtimeTook = Optional.of(timeTook);
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
