package io.logz.sawmill.exceptions;

public class ProcessorExecutionException extends SawmillException {
    public ProcessorExecutionException(String processorName, String errorMsg) {
        super(String.format("failed to processor %s, %s", processorName, errorMsg));
    }

    public ProcessorExecutionException(String processorName, String errorMsg, Exception e) {
        super(String.format("failed to processor %s, %s", processorName, errorMsg), e);
    }
}
