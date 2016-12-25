package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class PipelineExecutor {
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);

    private final PipelineExecutionTimeWatchdog watchdog;
    private final PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker;

    public PipelineExecutor(PipelineExecutionTimeWatchdog watchdog, PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker) {
        this.watchdog = watchdog;
        this.pipelineExecutionMetricsTracker = pipelineExecutionMetricsTracker;
    }

    public ExecutionResult execute(Pipeline pipeline, Doc doc) {
        PipelineStopwatch pipelineStopwatch = new PipelineStopwatch().start();

        long executionIdentifier = watchdog.startedExecution(new ExecutionContext(doc, pipeline.getId(), System.currentTimeMillis()));

        try {
            for (ExecutionStep executionStep : pipeline.getExecutionSteps()) {
                try {
                    ExecutionResult executionResult = executeStep(pipeline, doc, pipelineStopwatch, executionStep);

                    if (!executionResult.isSucceeded()) {
                         if (pipeline.isIgnoreFailure()) {
                             continue;
                         }

                         return executionResult;
                     }
                } catch (RuntimeException e) {
                    pipelineExecutionMetricsTracker.pipelineFailedOnUnexpectedError(pipeline.getId(), "", doc, e);
                    throw new PipelineExecutionException(pipeline.getName(), e);
                }
            }
            logger.trace("pipeline {} executed successfully, took {}ns", pipeline.getId(), pipelineStopwatch.pipelineElapsed());
        }
        finally {
            pipelineStopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        pipelineExecutionMetricsTracker.pipelineFinishedSuccessfully(pipeline.getId(), doc, pipelineStopwatch.pipelineElapsed());

        return ExecutionResult.success();
    }

    private ExecutionResult executeStep(Pipeline pipeline, Doc doc, PipelineStopwatch pipelineStopwatch, ExecutionStep executionStep) {
        if (executionStep instanceof ProcessorExecutionStep) {
            return execProcessorStep(pipeline, doc, pipelineStopwatch, (ProcessorExecutionStep) executionStep);
        } else if (executionStep instanceof ConditionalExecutionStep) {
            return execConditionalStep(pipeline, doc, pipelineStopwatch, (ConditionalExecutionStep) executionStep);
        }

        return ExecutionResult.failure("invalid execution step.", String.valueOf(executionStep));
    }

    private ExecutionResult execConditionalStep(Pipeline pipeline, Doc doc, PipelineStopwatch pipelineStopwatch, ConditionalExecutionStep conditionalExecutionStep) {
        Condition condition = conditionalExecutionStep.getCondition();
        List<ExecutionStep> conditionalNextSteps;
        if (condition.evaluate(doc)) {
            conditionalNextSteps = conditionalExecutionStep.getOnTrue();
        } else {
            conditionalNextSteps = conditionalExecutionStep.getOnFalse();
        }
        for (ExecutionStep step : conditionalNextSteps) {
            ExecutionResult result = executeStep(pipeline, doc, pipelineStopwatch, step);
            if (!result.isSucceeded()) return result;
        }
        return ExecutionResult.success();
    }

    private ExecutionResult execProcessorStep(Pipeline pipeline, Doc doc, PipelineStopwatch pipelineStopwatch, ProcessorExecutionStep executionStep) {
        Processor processor = executionStep.getProcessor();

        ProcessResult processResult = executeProcessor(doc, processor, pipelineStopwatch, pipeline.getId(), executionStep.getProcessorName());

        if (processResult.isSucceeded() || pipeline.isIgnoreFailure()) {
            return ExecutionResult.success();
        }

        if (executionStep.getOnFailureExecutionSteps().isPresent()) {
            executeOnFailure(doc, executionStep.getOnFailureExecutionSteps().get(), pipelineStopwatch, pipeline.getId());
            return ExecutionResult.success();
        }

        pipelineExecutionMetricsTracker.pipelineFailed(pipeline.getId(), doc);
        ProcessResult.Error error = processResult.getError().get();
        return ExecutionResult.failure(error.getMessage(),
                executionStep.getProcessorName(),
                error.getException().isPresent() ?
                        new PipelineExecutionException(pipeline.getName(), error.getException().get()) :
                        null);
    }

    private ProcessResult executeProcessor(Doc doc, Processor processor, PipelineStopwatch pipelineStopwatch, String pipelineId, String processorName) {
        pipelineStopwatch.startProcessor();
        ProcessResult processResult = processor.process(doc);
        long processorTook = pipelineStopwatch.processorElapsed();

        if (processResult.isSucceeded()) {
            logger.trace("processor {} in pipeline {} executed successfully, took {}ns", processorName, pipelineId, processorTook);
            pipelineExecutionMetricsTracker.processorFinishedSuccessfully(pipelineId, processorName, processorTook);
        } else {
            pipelineExecutionMetricsTracker.processorFailed(pipelineId, processorName, doc);
        }

        return processResult;
    }

    private void executeOnFailure(Doc doc, List<OnFailureExecutionStep> onFailureExecutionSteps, PipelineStopwatch pipelineStopwatch, String pipelineId) {
        for (OnFailureExecutionStep executionStep : onFailureExecutionSteps) {
            executeProcessor(doc, executionStep.getProcessor(), pipelineStopwatch, pipelineId, executionStep.getProcessorName());
        }
    }

    private static class PipelineStopwatch {
        private Stopwatch stopwatch;
        private long processorStartElapsedTime;
        private TimeUnit timeUnit = NANOSECONDS;

        public PipelineStopwatch() {
        }

        public PipelineStopwatch start() {
            stopwatch = Stopwatch.createStarted();
            processorStartElapsedTime = 0;
            return this;
        }

        public long pipelineElapsed() {
            return stopwatch.elapsed(timeUnit);
        }

        public long processorElapsed() {
            return stopwatch.elapsed(timeUnit) - processorStartElapsedTime;
        }

        public void startProcessor() {
            processorStartElapsedTime = stopwatch.elapsed(timeUnit);
        }

        public void stop() {
            stopwatch.stop();
        }
    }
}
