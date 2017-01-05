package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
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

        ExecutionResult executionResult;
        try {
            List<ExecutionStep> executionSteps = pipeline.getExecutionSteps();
            executionResult = executeSteps(executionSteps, pipeline, doc, pipelineStopwatch);

        } catch (RuntimeException e) {
            pipelineExecutionMetricsTracker.pipelineFailedOnUnexpectedError(pipeline.getId(), doc, e);
            throw new PipelineExecutionException(pipeline.getName(), e);

        } finally {
            pipelineStopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        if (!executionResult.isSucceeded()) {
            pipelineExecutionMetricsTracker.pipelineFailed(pipeline.getId(), doc);
            return executionResult;
        }

        logger.trace("pipeline {} executed successfully, took {}ns", pipeline.getId(), pipelineStopwatch.pipelineElapsed());
        pipelineExecutionMetricsTracker.pipelineFinishedSuccessfully(pipeline.getId(), doc, pipelineStopwatch.pipelineElapsed());

        return ExecutionResult.success();
    }

    private ExecutionResult executeSteps(List<ExecutionStep> executionSteps, Pipeline pipeline, Doc doc, PipelineStopwatch pipelineStopwatch) {
        for (ExecutionStep executionStep : executionSteps) {
            ExecutionResult executionResult = executeStep(executionStep, pipeline, doc, pipelineStopwatch);
            if (!executionResult.isSucceeded()) {
                return executionResult;
            }
        }
        return ExecutionResult.success();
    }

    private ExecutionResult executeStep(ExecutionStep executionStep, Pipeline pipeline, Doc doc, PipelineStopwatch pipelineStopwatch) {
        if (executionStep instanceof ConditionalExecutionStep) {
            return executeConditionalStep((ConditionalExecutionStep) executionStep, pipeline, doc, pipelineStopwatch);
        } else if (executionStep instanceof ProcessorExecutionStep) {
            return executeProcessorStep((ProcessorExecutionStep) executionStep, pipeline, doc, pipelineStopwatch);
        }

        throw new RuntimeException("Unsupported execution step " + executionStep.getClass());
    }

    private ExecutionResult executeConditionalStep(ConditionalExecutionStep conditionalExecutionStep, Pipeline pipeline, Doc doc, PipelineStopwatch pipelineStopwatch) {
        Condition condition = conditionalExecutionStep.getCondition();

        if (condition.evaluate(doc)) {
            return executeSteps(conditionalExecutionStep.getOnTrue(), pipeline, doc, pipelineStopwatch);
        } else {
            return executeSteps(conditionalExecutionStep.getOnFalse(), pipeline, doc, pipelineStopwatch);
        }
    }

    private ExecutionResult executeProcessorStep(ProcessorExecutionStep executionStep, Pipeline pipeline, Doc doc, PipelineStopwatch pipelineStopwatch) {
        Processor processor = executionStep.getProcessor();

        ProcessResult processResult = executeProcessor(doc, processor, pipelineStopwatch, pipeline.getId(), executionStep.getProcessorName());

        if (processResult.isSucceeded() || pipeline.isIgnoreFailure()) {
            return ExecutionResult.success();
        }

        Optional<List<OnFailureExecutionStep>> onFailureExecutionSteps = executionStep.getOnFailureExecutionSteps();
        if (onFailureExecutionSteps.isPresent()) {
            executeOnFailure(onFailureExecutionSteps.get(), doc, pipelineStopwatch, pipeline.getId());
            return ExecutionResult.success();
        }

        ProcessResult.Error error = processResult.getError().get();
        String message = error.getMessage();
        String processorName = executionStep.getProcessorName();
        Optional<ProcessorExecutionException> exception = error.getException();
        PipelineExecutionException e = exception.isPresent() ? new PipelineExecutionException(pipeline.getName(), exception.get()) : null;
        return ExecutionResult.failure(message, processorName, e);
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

    private void executeOnFailure(List<OnFailureExecutionStep> onFailureExecutionSteps, Doc doc, PipelineStopwatch pipelineStopwatch, String pipelineId) {
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
