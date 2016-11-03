package metrics;

import io.logz.sawmill.PipelineExecutorMBean;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

public class PipelineExecutorMetrics implements PipelineExecutorMBean {
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder overtime = new LongAdder();
    private final ConcurrentMap<String, ProcessorMetrics> processorsMetrics = new ConcurrentHashMap<>();

    @Override
    public long getTotal() { return succeeded.longValue() + failed.longValue() + overtime.longValue(); }

    @Override
    public long getSucceeded() { return succeeded.longValue(); }

    @Override
    public long getFailed() { return failed.longValue(); }

    @Override
    public long getOvertime() { return overtime.longValue(); }

    @Override
    public float getAvgProcessorProcessingTime(String processorName) { return processorsMetrics.get(processorName).getAvgTime(); }

    @Override
    public long getMinProcessorProcessingTime(String processorName) { return processorsMetrics.get(processorName).getMinTime(); }

    @Override
    public long getMaxProcessorProcessingTime(String processorName) { return processorsMetrics.get(processorName).getMaxTime(); }

    @Override
    public void incrementSucceeded() { succeeded.increment(); }

    @Override
    public void incrementFailed() { failed.increment(); }

    @Override
    public void incrementOvertime() { overtime.increment(); }

    @Override
    public void processorFinished(String processorName, long duration) {
        processorsMetrics.computeIfAbsent(processorName, k -> new ProcessorMetrics()).addEvent(duration);
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
