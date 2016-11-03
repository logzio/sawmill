package io.logz.sawmill;

public interface PipelineExecutorMBean {
    long getTotal();

    long getSucceeded();

    long getFailed();

    long getOvertime();

    float getAvgProcessorProcessingTime(String processorName);

    long getMinProcessorProcessingTime(String processorName);

    long getMaxProcessorProcessingTime(String processorName);

    void incrementSucceeded();

    void incrementFailed();

    void incrementOvertime();

    void processorFinished(String processorName, long duration);
}
