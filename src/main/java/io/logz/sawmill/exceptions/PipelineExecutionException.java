package io.logz.sawmill.exceptions;

public class PipelineExecutionException extends Throwable {

    public PipelineExecutionException(String errorMsg, Exception e) {
        super(errorMsg, e);
    }
}
