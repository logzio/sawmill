package io.logz.sawmill.executor;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ExecutionResult;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutionMetricsMBean;
import io.logz.sawmill.PipelineExecutionMetricsTracker;
import io.logz.sawmill.PipelineExecutionTimeWatchdog;
import io.logz.sawmill.PipelineExecutor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SawmillExecutor {
    private static final long WARNING_THRESHOLD_TIME_MS = 1000;
    private final PipelineExecutionMetricsTracker pipelineMetrics;
    private final PipelineExecutor pipelineExecutor;

    public SawmillExecutor() {
        this(new PipelineExecutionMetricsMBean());
    }

    public SawmillExecutor(PipelineExecutionMetricsTracker pipelineMetrics) {
        this(pipelineMetrics, new PipelineExecutionTimeWatchdog(WARNING_THRESHOLD_TIME_MS,
                pipelineMetrics, null));
    }

    public SawmillExecutor(PipelineExecutionMetricsTracker pipelineMetrics,
                           PipelineExecutionTimeWatchdog watchdog) {
        this.pipelineMetrics = pipelineMetrics;
        this.pipelineExecutor = new PipelineExecutor(watchdog, pipelineMetrics);
    }

    public void execute(ExecutionPlan executionPlan) {
        Input input = executionPlan.getInput();
        Pipeline pipeline = executionPlan.getPipeline();
        Output output = executionPlan.getOutput();

        List<Doc> docs = input.listen();

        Map<Doc, ExecutionResult> executionResults = docs.stream().collect(Collectors.toMap(Function.identity(), doc -> pipelineExecutor.execute(pipeline, doc)));

        List<Doc> succeededDocs = executionResults.entrySet().stream()
                .filter(entry -> !entry.getValue().isDropped())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        output.send(succeededDocs);
    }
}
