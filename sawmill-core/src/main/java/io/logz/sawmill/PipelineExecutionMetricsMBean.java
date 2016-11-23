package io.logz.sawmill;

import org.weakref.jmx.Managed;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

public class PipelineExecutionMetricsMBean implements PipelineExecutionMetricsTracker {
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder overtime = new LongAdder();
    private final LongAdder unexpectedFailure = new LongAdder();
    private final ConcurrentMap<String, ProcessorMetrics> processorsMetrics = new ConcurrentHashMap<>();

    @Managed
    @Override
    public long totalDocsProcessed() { return succeeded.longValue() + failed.longValue() + overtime.longValue(); }

    @Managed
    @Override
    public long totalDocsSucceededProcessing() { return succeeded.longValue(); }

    @Managed
    @Override
    public long totalDocsFailedProcessing() { return failed.longValue(); }

    @Managed
    @Override
    public long totalDocsOvertimeProcessing() { return overtime.longValue(); }

    @Managed
    @Override
    public long totalDocsFailedOnUnexpectedError() {
        return unexpectedFailure.longValue();
    }

    @Managed
    @Override
    public float getAvgProcessorProcessingTime(String processorType) { return processorsMetrics.get(processorType).getAvgTime(); }

    @Managed
    @Override
    public long getMinProcessorProcessingTime(String processorType) { return processorsMetrics.get(processorType).getMinTime(); }

    @Managed
    @Override
    public long getMaxProcessorProcessingTime(String processorType) { return processorsMetrics.get(processorType).getMaxTime(); }

    @Override
    public void processedDocSuccessfully(String pipelineId, Doc doc, long timeTookNs) { succeeded.increment(); }

    @Override
    public void processorFailed(String pipelineId, String processorType, Doc doc) { failed.increment(); }

    @Override
    public void overtimeProcessingDoc(String pipelineId, Doc doc) { overtime.increment(); }

    @Override
    public void processorFinished(String processorType, long timeTookNs) {
        processorsMetrics.computeIfAbsent(processorType, k -> new ProcessorMetrics()).addEvent(timeTookNs);
    }

    @Override
    public void processorFailedOnUnexpectedError(String pipelineId, String processorType, Doc doc, Exception e) {
        unexpectedFailure.increment();
    }

    private class ProcessorMetrics {
        LongAdder eventsCount = new LongAdder();
        LongAdder eventsDuration = new LongAdder();
        long maxTime = 0;
        long minTime = 0;

        public void addEvent(long duration) {
            eventsCount.increment();
            eventsDuration.add(duration);

            if (maxTime == 0 || duration > maxTime) {
                maxTime = duration;
            }
            if (minTime == 0 || duration < minTime) {
                minTime = duration;
            }
        }

        public long getMaxTime() { return maxTime; }

        public long getMinTime() { return minTime; }

        public float getAvgTime() {
            float avgTime = 0;
            long totalEventsCount = eventsCount.longValue();
            if (totalEventsCount > 0) {
                long totalEventsDuration = eventsDuration.longValue();
                avgTime = totalEventsDuration / totalEventsCount;
            }
            return avgTime;
        }
    }
}
