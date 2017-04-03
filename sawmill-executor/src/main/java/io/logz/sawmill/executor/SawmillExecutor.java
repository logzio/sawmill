package io.logz.sawmill.executor;

import io.logz.sawmill.PipelineExecutionMetricsMBean;
import io.logz.sawmill.PipelineExecutionMetricsTracker;
import io.logz.sawmill.PipelineExecutionTimeWatchdog;
import io.logz.sawmill.PipelineExecutor;

import java.util.Map;

public class SawmillExecutor {
    private static final long WARNING_THRESHOLD_TIME_MS = 1000;
    private final PipelineExecutionMetricsTracker pipelineMetrics;
    private final PipelineExecutor pipelineExecutor;

    public SawmillExecutor() {
        this.pipelineMetrics = new PipelineExecutionMetricsMBean();
        PipelineExecutionTimeWatchdog watchdog = new PipelineExecutionTimeWatchdog(WARNING_THRESHOLD_TIME_MS,
                pipelineMetrics, null);

        this.pipelineExecutor = new PipelineExecutor(watchdog, pipelineMetrics);
    }

    public void start(Map<String, Object> config) {

    }
}
