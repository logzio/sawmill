package io.logz.sawmill;

public interface PipelineExecutionMetricsTracker {
    void pipelineFinishedSuccessfully(String pipelineId, Doc doc, long timeTookNs);

    void pipelineFailed(String pipelineId, Doc doc);

    void pipelineExpired(String pipelineId, Doc doc);

    void docDropped(String pipelineId, Doc doc);

    void processorFinishedSuccessfully(String pipelineId, String processorName, long timeTookNs);

    void processorFailed(String pipelineId, String processorName, Doc doc, ProcessResult.Error error);

    void pipelineFailedOnUnexpectedError(String pipelineId, Doc doc, Exception e);

    void overtimeProcessingDoc(String pipelineId, Doc doc);
}
