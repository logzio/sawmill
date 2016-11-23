package io.logz.sawmill;

public interface PipelineExecutionMetricsTracker {
    long totalDocsProcessed();

    long totalDocsSucceededProcessing();

    long totalDocsFailedProcessing();

    long totalDocsOvertimeProcessing();

    long totalDocsFailedOnUnexpectedError();

    float getAvgProcessorProcessingTime(String processorType);

    long getMinProcessorProcessingTime(String processorType);

    long getMaxProcessorProcessingTime(String processorType);

    void processedDocSuccessfully(String pipelineId, Doc doc, long timeTookNs);

    void processorFailed(String pipelineId, String processorType, Doc doc);

    void processorFailedOnUnexpectedError(String pipelineId, String processorType, Doc doc, Exception e);

    void overtimeProcessingDoc(String pipelineId, Doc doc);

    void processorFinished(String processorType, long timeTookNs);
}
