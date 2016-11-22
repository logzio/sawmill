package io.logz.sawmill;

public interface PipelineExecutionMetricsTracker {
    long totalDocsProcessed();

    long totalDocsSucceededProcessing();

    long totalDocsFailedProcessing();

    long totalDocsOvertimeProcessing();

    long totalDocsFailedOnUnexpectedError();

    long totalDocsDropped();

    float getAvgProcessorProcessingTime(String processorName);

    long getMinProcessorProcessingTime(String processorName);

    long getMaxProcessorProcessingTime(String processorName);

    void processedDocSuccessfully(String pipelineId, Doc doc, long timeTookNs);

    void processorFailed(String pipelineId, String processorName, Doc doc);

    void processorFailedOnUnexpectedError(String pipelineId, String processorName, Doc doc, Exception e);

    void overtimeProcessingDoc(String pipelineId, Doc doc);

    void processorFinished(String processorName, long timeTookNs);

    void docDropped(Doc doc);
}
