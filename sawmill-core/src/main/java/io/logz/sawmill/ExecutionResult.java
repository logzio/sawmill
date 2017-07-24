package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;

import java.util.Optional;

import static io.logz.sawmill.Result.DROPPED;
import static io.logz.sawmill.Result.FAILED;
import static io.logz.sawmill.Result.SUCCEEDED;

public class ExecutionResult {
    private final Result result;
    private final Optional<Error> error;
    private Optional<Long> timeTook;

    private ExecutionResult() {
        this(SUCCEEDED);
    }

    private ExecutionResult(Result result) {
        this.result = result;
        this.error = Optional.empty();
        this.timeTook = Optional.empty();
    }

    private ExecutionResult(String errorMessage, String failedProcessorName) {
        this(errorMessage, failedProcessorName, null);
    }

    private ExecutionResult(String errorMessage, String failedProcessorName, PipelineExecutionException e) {
        this.result = FAILED;
        this.error = Optional.of(new Error(errorMessage, failedProcessorName, Optional.ofNullable(e)));
        this.timeTook = Optional.empty();
    }

    public static ExecutionResult success() {
        return new ExecutionResult();
    }

    public static ExecutionResult failure(String errorMessage, String failedProcessorName) {
        return new ExecutionResult(errorMessage, failedProcessorName);
    }

    public static ExecutionResult failure(String errorMessage, String failedProcessorName, PipelineExecutionException e) {
        return new ExecutionResult(errorMessage, failedProcessorName, e);
    }

    public boolean isSucceeded() {
        return result == SUCCEEDED;
    }
    public boolean isDropped() {
        return result == DROPPED;
    }
    public boolean isOvertime() {
        return timeTook.isPresent();
    }

    public static ExecutionResult dropped() {
        return new ExecutionResult(DROPPED);
    }

    public Optional<Error> getError() {
        return error;
    }

    public Optional<Long> getTimeTook() {
        return timeTook;
    }

    public void setOvertime(long timeTook) {
        this.timeTook = Optional.of(timeTook);
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
