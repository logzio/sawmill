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
    private final LongAdder dropped = new LongAdder();
    private final LongAdder expired = new LongAdder();
    private final ConcurrentMap<String, ProcessorMetrics> processorsMetrics = new ConcurrentHashMap<>();

    @Managed
    public long getTotalDocsProcessed() {
        return succeeded.longValue() + failed.longValue() + overtime.longValue();
    }

    @Managed
    public long getTotalDocsSucceededProcessing() {
        return succeeded.longValue();
    }

    @Managed
    public long getTotalDocsFailedProcessing() {
        return failed.longValue();
    }

    @Managed
    public long getTotalDocsOvertimeProcessing() {
        return overtime.longValue();
    }

    @Managed
    public long getTotalDocsProcessingExpired() {
        return expired.longValue();
    }

    @Managed
    public long getTotalDocsFailedOnUnexpectedError() {
        return unexpectedFailure.longValue();
    }

    @Managed
    public long getTotalDocsDropped() {
        return dropped.longValue();
    }

    @Managed
    public float getAvgProcessorProcessingTime(String processorName) {
        return processorsMetrics.get(processorName).getAvgTime();
    }

    @Managed
    public long getMinProcessorProcessingTime(String processorName) {
        return processorsMetrics.get(processorName).getMinTime();
    }

    @Managed
    public long getMaxProcessorProcessingTime(String processorName) {
        return processorsMetrics.get(processorName).getMaxTime();
    }
    @Managed
    public long getProcessingFailedCount(String processorName) {
        return processorsMetrics.get(processorName).getFailed();
    }

    @Override
    public void pipelineFinishedSuccessfully(String pipelineId, Doc doc, long timeTookNs) {
        succeeded.increment();
    }

    @Override
    public void processorFailed(String pipelineId, String processorName, Doc doc, ProcessResult.Error error) {
        processorsMetrics.computeIfAbsent(processorName, k -> new ProcessorMetrics()).incrementFailure();
    }

    @Override
    public void overtimeProcessingDoc(String pipelineId, Doc doc) {
        overtime.increment();
    }

    @Override
    public void pipelineFailed(String pipelineId, Doc doc) {
        failed.increment();
    }

    @Override
    public void docDropped(String pipelineId, Doc doc) {
        dropped.increment();
    }

    @Override
    public void pipelineExpired(String pipelineId, Doc doc) {
        expired.increment();
    }

    @Override
    public void processorFinishedSuccessfully(String pipelineId, String processorName, long timeTookNs) {
        processorsMetrics.computeIfAbsent(processorName, k -> new ProcessorMetrics()).addEvent(timeTookNs);
    }

    @Override
    public void pipelineFailedOnUnexpectedError(String pipelineId, Doc doc, Exception e) {
        unexpectedFailure.increment();
    }

    private class ProcessorMetrics {
        LongAdder eventsCount = new LongAdder();
        LongAdder eventsDuration = new LongAdder();
        LongAdder eventsFailed = new LongAdder();
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

        public void incrementFailure() {
            eventsFailed.increment();
        }

        public long getFailed() {
            return eventsFailed.longValue();
        }

        public long getMaxTime() {
            return maxTime;
        }

        public long getMinTime() {
            return minTime;
        }

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
