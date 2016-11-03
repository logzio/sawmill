package io.logz.sawmill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutionTimeWatchdog {
    public static final int THRESHOLD_CHECK_FACTOR = 10;

    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutionTimeWatchdog.class);


    private final long thresholdTimeMs;
    private final ConcurrentMap<Long, ExecutionContext> currentlyRunning;
    private final Consumer<ExecutionContext> overtimeOp;
    private final AtomicLong executionIdGenerator;

    public PipelineExecutionTimeWatchdog(long thresholdTimeMs, Consumer<ExecutionContext> overtimeOp) {
        this.overtimeOp = overtimeOp;
        this.thresholdTimeMs = thresholdTimeMs;
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
            currentlyRunning.values().stream().filter(context -> now - context.getIngestTimestamp() > thresholdTimeMs).forEach(overtimeOp);
        } catch (Exception e) {
            logger.error("failed to alert of overtime executions", e);
        }
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
