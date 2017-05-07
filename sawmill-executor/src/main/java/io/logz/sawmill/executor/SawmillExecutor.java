package io.logz.sawmill.executor;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ExecutionResult;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutionMetricsMBean;
import io.logz.sawmill.PipelineExecutionMetricsTracker;
import io.logz.sawmill.PipelineExecutionTimeWatchdog;
import io.logz.sawmill.PipelineExecutor;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SawmillExecutor {
    private static final long WARNING_THRESHOLD_TIME_MS = 1000;
    private final PipelineExecutionMetricsTracker pipelineMetrics;
    private final PipelineExecutor pipelineExecutor;
    private final ExecutionPlan executionPlan;
    private final ExecutorService executor;

    public SawmillExecutor(String config) {
        this.pipelineMetrics = new PipelineExecutionMetricsMBean();
        PipelineExecutionTimeWatchdog watchdog = new PipelineExecutionTimeWatchdog(WARNING_THRESHOLD_TIME_MS,
                pipelineMetrics, null);

        this.pipelineExecutor = new PipelineExecutor(watchdog, pipelineMetrics);
        this.executionPlan = new ExecutionPlan.Factory().create(config);
        this.executor = Executors.newFixedThreadPool(1);

    }

    public void start() {
        Input input = executionPlan.getInput();
        Pipeline pipeline = executionPlan.getPipeline();
        Output output = executionPlan.getOutput();

        Future<Doc> future = executor.submit(input.listen());

        try {
            ExecutionResult executionResult = pipelineExecutor.execute(pipeline, future.get());

            if (executionResult.isSucceeded()) {
                output.send(future.get());
            }
        } catch (InterruptedException e) {

        } catch (ExecutionException e) {

        }
    }
}
