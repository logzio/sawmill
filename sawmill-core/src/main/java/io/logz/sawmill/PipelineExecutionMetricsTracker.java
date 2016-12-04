package io.logz.sawmill;

public interface PipelineExecutionMetricsTracker {
    float getAvgProcessorProcessingTime(String processorType);

    long getMinProcessorProcessingTime(String processorType);

    long getMaxProcessorProcessingTime(String processorType);

    void processedDocSuccessfully(String pipelineId, Doc doc, long timeTookNs);

    void processorFailed(String pipelineId, String processorName, Doc doc);

    void processorFailedOnUnexpectedError(String pipelineId, String processorName, Doc doc, Exception e);

    void overtimeProcessingDoc(String pipelineId, Doc doc);

    void processorFinished(String pipelineId, String processorName, long timeTookNs);
}
