package io.logz.sawmill;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutionTimeWatchdog {
    public static final int THRESHOLD_CHECK_FACTOR = 10;

    private final long thresholdTimeMs;
    private final ConcurrentMap<String, ExecutionContext> currentlyRunning;
    private final Consumer<ExecutionContext> overtimeOp;

    public PipelineExecutionTimeWatchdog(long thresholdTimeMs, Consumer<ExecutionContext> overtimeOp) {
        this.overtimeOp = overtimeOp;
        this.thresholdTimeMs = thresholdTimeMs;
        this.currentlyRunning = new ConcurrentHashMap<>();
        initWatchdog(thresholdTimeMs / THRESHOLD_CHECK_FACTOR);
    }

    private void initWatchdog(long periodMs) {
        ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
        timer.scheduleAtFixedRate(this::alertOvertimeExecutions, 0, periodMs, MILLISECONDS);
    }

    private void alertOvertimeExecutions() {
        long now = System.currentTimeMillis();
        currentlyRunning.values().stream().filter(context -> now - context.getIngestTimestamp() > thresholdTimeMs).forEach(overtimeOp);
    }

    public String startedExecution(ExecutionContext context) {
        String id = UUID.randomUUID().toString();
        currentlyRunning.put(id, context);

        return id;
    }

    public void removeExecution(String id) {
        currentlyRunning.remove(id);
    }
}
