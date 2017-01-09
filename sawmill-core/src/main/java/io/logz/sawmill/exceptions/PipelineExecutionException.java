package io.logz.sawmill.exceptions;

public class PipelineExecutionException extends SawmillException {
    public PipelineExecutionException(String pipelineId, Exception e) {
        super(String.format("failed to execute pipeline %s", pipelineId), e);
    }
}
