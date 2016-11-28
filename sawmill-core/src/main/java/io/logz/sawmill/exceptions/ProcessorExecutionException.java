package io.logz.sawmill.exceptions;

public class ProcessorExecutionException extends SawmillException {
    public ProcessorExecutionException(String processorType, Exception e) {
        super(String.format("failed to execute processor %s", processorType), e);
    }
}
