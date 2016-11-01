package io.logz.sawmill.exceptions;

public class PipelineExecutionException extends SawmillException {
    public PipelineExecutionException(String pipelineName, String processorName, Exception e) {
        super(String.format("failed to execute pipeline %s, processor %s failed", pipelineName, processorName), e);
    }
}
