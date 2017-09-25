package io.logz.sawmill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutionTimeWatchdog implements Closeable {
    public static final int THRESHOLD_CHECK_FACTOR = 10;

    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutionTimeWatchdog.class);

    private final long warningThresholdTimeMs;
    private final long expiredThresholdTimeMs;
    private final ConcurrentMap<Long, WatchedPipeline> currentlyRunning;
    private final Consumer<WatchedPipeline> overtimeOp;
    private final PipelineExecutionMetricsTracker metricsTracker;
    private final AtomicLong executionIdGenerator;
    private ScheduledExecutorService timer;

    public PipelineExecutionTimeWatchdog(long warningThresholdTimeMs, long expiredThresholdTimeMs, PipelineExecutionMetricsTracker metricsTracker, Consumer<WatchedPipeline> overtimeOp) {
        this.warningThresholdTimeMs = warningThresholdTimeMs;
        this.expiredThresholdTimeMs = expiredThresholdTimeMs;
        this.metricsTracker = metricsTracker;
        this.overtimeOp = overtimeOp;
        this.currentlyRunning = new ConcurrentHashMap<>();
        this.executionIdGenerator = new AtomicLong();
        initWatchdog(warningThresholdTimeMs / THRESHOLD_CHECK_FACTOR);
    }

    private void initWatchdog(long periodMs) {
        timer = Executors.newScheduledThreadPool(1);
        timer.scheduleAtFixedRate(this::alertOvertimeExecutions, 0, periodMs, MILLISECONDS);
    }

    private void alertOvertimeExecutions() {
        try {
            warnOvertimeExecutions();
            killExpiredExecutions();
        } catch (Exception e) {
            logger.error("failed to alert of overtime executions", e);
        }
    }

    private void killExpiredExecutions() {
        long now = System.currentTimeMillis();
        List<WatchedPipeline> expiredExecutions = currentlyRunning.values().stream()
                .filter(watchedPipeline -> now - watchedPipeline.getIngestTimestamp() > expiredThresholdTimeMs)
                .collect(Collectors.toList());

        expiredExecutions.forEach(this::interruptIfRunning);
    }

    private void warnOvertimeExecutions() {
        long now = System.currentTimeMillis();
        List<WatchedPipeline> warningExceededExecutions = currentlyRunning.values().stream()
                .filter(watchedPipeline -> now - watchedPipeline.getIngestTimestamp() > warningThresholdTimeMs)
                .filter(watchedPipeline -> !watchedPipeline.hasBeenNotifiedAsOvertime())
                .collect(Collectors.toList());
        warningExceededExecutions.forEach(this::notifyOvertimeToMetricsTracker);
        warningExceededExecutions.forEach(overtimeOp);
        warningExceededExecutions.forEach(WatchedPipeline::setAsNotifiedWithOvertime);
    }

    /***
     * Stop watching pipeline and interrupt if needed
     * Check whether the execution has been stopped already
     * @param watchedPipeline
     * @param shouldInterrupt indicate if interrupt is required
     * @return {@code true} if already finished.
     */
    private synchronized boolean stopWatchedPipeline(WatchedPipeline watchedPipeline, boolean shouldInterrupt) {
        boolean alreadyFinished = !watchedPipeline.compareAndSetFinishedRunning();

        if (shouldInterrupt && !alreadyFinished) {
            watchedPipeline.interrupt();
            notifyExpiredToMetricsTracker(watchedPipeline);
        }

        return alreadyFinished;
    }

    private void interruptIfRunning(WatchedPipeline watchedPipeline) {
        stopWatchedPipeline(watchedPipeline, true);
    }

    public boolean stopWatchedPipeline(long executionIdentifier) {
        return stopWatchedPipeline(currentlyRunning.get(executionIdentifier), false);
    }

    private void notifyOvertimeToMetricsTracker(WatchedPipeline watchedPipeline) {
        metricsTracker.overtimeProcessingDoc(watchedPipeline.getPipelineId(), watchedPipeline.getDoc());
    }

    private void notifyExpiredToMetricsTracker(WatchedPipeline watchedPipeline) {
        metricsTracker.pipelineExpired(watchedPipeline.getPipelineId(), watchedPipeline.getDoc());
    }

    public long startedExecution(String pipelineId, Doc doc, Thread context) {
        long ingestTimestamp = System.currentTimeMillis();
        WatchedPipeline watchedPipeline = new WatchedPipeline(doc, pipelineId, ingestTimestamp, context);
        long id = executionIdGenerator.incrementAndGet();
        currentlyRunning.put(id, watchedPipeline);

        return id;
    }

    public void removeExecution(long id) {
        currentlyRunning.remove(id);
    }

    @Override
    public void close() {
        try {
            timer.shutdown();
            timer.awaitTermination(1, TimeUnit.SECONDS);
            timer.shutdownNow();

        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    public boolean isOvertime(long executionIdentifier) {
        return currentlyRunning.get(executionIdentifier).hasBeenNotifiedAsOvertime();
    }
}
