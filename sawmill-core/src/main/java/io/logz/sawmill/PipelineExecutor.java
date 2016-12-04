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
                    Processor processor = executionStep.getProcessor();

                    ProcessResult processResult = executeProcessor(doc, processor, pipelineStopwatch, pipeline.getId(), executionStep.getProcessorName());

                    if (!processResult.isSucceeded()) {
                         if (pipeline.isIgnoreFailure()) {
                             continue;
                         }

                         if (!executionStep.getOnFailureExecutionSteps().isPresent()) {
                             ProcessResult.Error error = processResult.getError().get();
                             return ExecutionResult.failure(error.getMessage(),
                                     executionStep.getProcessorName(),
                                     error.getException().isPresent() ?
                                             new PipelineExecutionException(pipeline.getName(), error.getException().get()) :
                                             null);
                         } else {
                             executeOnFailure(doc, executionStep.getOnFailureExecutionSteps().get(), pipelineStopwatch, pipeline.getId(), executionStep.getProcessorName());
                         }
                     }
                } catch (RuntimeException e) {
                    pipelineExecutionMetricsTracker.processorFailedOnUnexpectedError(pipeline.getId(), executionStep.getProcessorName(), doc, e);
                    throw new PipelineExecutionException(pipeline.getName(), e);
                }
            }
            logger.trace("pipeline {} executed successfully, took {}ns", pipeline.getId(), pipelineStopwatch.pipelineElapsed());
        }
        finally {
            pipelineStopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        pipelineExecutionMetricsTracker.processedDocSuccessfully(pipeline.getId(), doc, pipelineStopwatch.pipelineElapsed());

        return ExecutionResult.success();
    }

    private ProcessResult executeProcessor(Doc doc, Processor processor, PipelineStopwatch pipelineStopwatch, String pipelineId, String processorName) {
        pipelineStopwatch.startProcessor();
        ProcessResult processResult = processor.process(doc);
        long processorTook = pipelineStopwatch.processorElapsed();

        if (processResult.isSucceeded()) {
            logger.trace("processor {} in pipeline {} executed successfully, took {}ns", processorName, pipelineId, processorTook);
            pipelineExecutionMetricsTracker.processorFinished(pipelineId, processorName, processorTook);
        } else {
            pipelineExecutionMetricsTracker.processorFailed(pipelineId, processorName, doc);
        }

        return processResult;
    }

    private void executeOnFailure(Doc doc, List<OnFailureExecutionStep> onFailureExecutionSteps, PipelineStopwatch pipelineStopwatch, String pipelineId, String processorName) {
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
