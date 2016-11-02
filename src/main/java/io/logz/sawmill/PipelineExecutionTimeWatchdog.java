package io.logz.sawmill;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutionTimeWatchdog {
    public static final int DIVIDE_FACTOR = 10;

    private final long thresholdTimeMs;
    private final ConcurrentMap<String, ExecutionContext> idToInfo;
    private final Consumer<ExecutionContext> overtimeOp;

    public PipelineExecutionTimeWatchdog(long thresholdTimeMs, Consumer<ExecutionContext> overtimeOp) {
        this.overtimeOp = overtimeOp;
        this.thresholdTimeMs = thresholdTimeMs;
        this.idToInfo = new ConcurrentHashMap<>();
        initWatchdog(thresholdTimeMs / DIVIDE_FACTOR);
    }

    private void initWatchdog(long periodMs) {
        ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
        timer.scheduleAtFixedRate(this::alertOvertimeExecutions, 0, periodMs, MILLISECONDS);
    }

    private void alertOvertimeExecutions() {
        long now = System.currentTimeMillis();
        idToInfo.values().stream().filter(context -> now - context.getIngestTimestamp() > thresholdTimeMs).forEach(overtimeOp);
    }

    public void addExecution(String id, ExecutionContext context) {
        idToInfo.put(id, context);
    }

    public void removeExecution(String id) {
        idToInfo.remove(id);
    }
}
