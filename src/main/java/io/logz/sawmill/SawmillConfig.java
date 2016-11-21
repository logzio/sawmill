package io.logz.sawmill;

public class SawmillConfig {
    public static final int DEFAULT_THRESHOLD_TIME_MS = 1000;
    private PipelineExecutionMetricsTracker metricsTracker;
    private PipelineExecutionTimeWatchdog watchdog;
    private long thresholdTimeMs;

    public SawmillConfig() {
        thresholdTimeMs = DEFAULT_THRESHOLD_TIME_MS;
        metricsTracker = new PipelineExecutionMetricsMBean();
        watchdog = new PipelineExecutionTimeWatchdog(thresholdTimeMs, metricsTracker,  context -> { });
    }

    public PipelineExecutionMetricsTracker getMetricsTracker() {
        return metricsTracker;
    }

    public void setMetricsTracker(PipelineExecutionMetricsTracker metricsTracker) {
        this.metricsTracker = metricsTracker;
    }

    public PipelineExecutionTimeWatchdog getWatchdog() {
        return watchdog;
    }

    public void setWatchdog(PipelineExecutionTimeWatchdog watchdog) {
        this.watchdog = watchdog;
    }

    public long getThresholdTimeMs() {
        return thresholdTimeMs;
    }

    public void setThresholdTimeMs(long thresholdTimeMs) {
        this.thresholdTimeMs = thresholdTimeMs;
    }
}
