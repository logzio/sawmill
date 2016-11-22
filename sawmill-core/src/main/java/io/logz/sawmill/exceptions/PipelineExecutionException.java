package io.logz.sawmill.exceptions;

public class PipelineExecutionException extends SawmillException {
    public PipelineExecutionException(String pipelineName, Exception e) {
        super(String.format("failed to execute pipeline %s", pipelineName), e);
    }
}
