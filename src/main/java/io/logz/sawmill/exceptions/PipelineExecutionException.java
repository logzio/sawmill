package io.logz.sawmill.exceptions;

public class PipelineExecutionException extends SawmillException {
    public PipelineExecutionException(String errorMsg, Exception e) {
        super(errorMsg, e);
    }
}
