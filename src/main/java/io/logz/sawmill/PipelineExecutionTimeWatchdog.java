package io.logz.sawmill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutionTimeWatchdog {
    public static final int THRESHOLD_CHECK_FACTOR = 10;

    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutionTimeWatchdog.class);

    private final long thresholdTimeMs;
    private final ConcurrentMap<Long, ExecutionContext> currentlyRunning;
    private final Consumer<ExecutionContext> overtimeOp;
    private final PipelineExecutionMetricsTracker metricsTracker;
    private final AtomicLong executionIdGenerator;

    public PipelineExecutionTimeWatchdog(long thresholdTimeMs, PipelineExecutionMetricsTracker metricsTracker, Consumer<ExecutionContext> overtimeOp) {
        this.thresholdTimeMs = thresholdTimeMs;
        this.metricsTracker = metricsTracker;
        this.overtimeOp = overtimeOp;
        this.currentlyRunning = new ConcurrentHashMap<>();
        this.executionIdGenerator = new AtomicLong();
        initWatchdog(thresholdTimeMs / THRESHOLD_CHECK_FACTOR);
    }

    private void initWatchdog(long periodMs) {
        ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
        timer.scheduleAtFixedRate(this::alertOvertimeExecutions, 0, periodMs, MILLISECONDS);
    }

    private void alertOvertimeExecutions() {
        try {
            long now = System.currentTimeMillis();
            List<ExecutionContext> overtimeExecutions = currentlyRunning.values().stream().filter(context -> now - context.getIngestTimestamp() > thresholdTimeMs).collect(Collectors.toList());
            overtimeExecutions.forEach(this::notifyMetricsTracker);
            overtimeExecutions.forEach(overtimeOp);
        } catch (Exception e) {
            logger.error("failed to alert of overtime executions", e);
        }
    }

    private void notifyMetricsTracker(ExecutionContext context) {
        metricsTracker.overtimeProcessingDoc(context.getPipelineId(), context.getDoc());
    }

    public long startedExecution(ExecutionContext context) {
        long id = executionIdGenerator.incrementAndGet();
        currentlyRunning.put(id, context);

        return id;
    }

    public void removeExecution(long id) {
        currentlyRunning.remove(id);
    }
}
