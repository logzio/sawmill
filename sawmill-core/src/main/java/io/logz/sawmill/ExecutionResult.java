package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;

import java.util.Optional;

public class ExecutionResult {
    private final boolean succeeded;
    private final String errorMessage;
    private final String processorType;
    private final String processorName;
    private final Optional<PipelineExecutionException> exception;

    public ExecutionResult(boolean succeeded) {
        this(succeeded, "", "", "");
    }

    public ExecutionResult(boolean succeeded, String errorMessage, String processorType, String processorName) {
        this(succeeded, errorMessage, processorType, processorName, Optional.empty());
    }

    public ExecutionResult(boolean succeeded, String errorMessage, String processorType, String processorName, Optional<PipelineExecutionException> e) {
        this.succeeded = succeeded;
        this.errorMessage = errorMessage;
        this.processorType = processorType;
        this.processorName = processorName;
        this.exception = e;
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getProcessorType() {
        return processorType;
    }

    public String getProcessorName() {
        return processorName;
    }

    public Optional<PipelineExecutionException> getException() {
        return exception;
    }
}
