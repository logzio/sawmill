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
        PipelineStopwatch pipelineStopwatch = new PipelineStopwatch(NANOSECONDS).start();

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

                         if (!executionStep.getOnFailureProcessors().isPresent()) {
                             ProcessResult.Error error = processResult.getError().get();
                             return ExecutionResult.failure(error.getMessage(),
                                     executionStep.getProcessorName(),
                                     error.getException().isPresent() ?
                                             new PipelineExecutionException(pipeline.getName(), error.getException().get()) :
                                             null);
                         } else {
                             executeOnFailure(doc, executionStep.getOnFailureProcessors().get(), pipelineStopwatch, pipeline.getId(), executionStep.getProcessorName());
                         }
                     }
                } catch (RuntimeException e) {
                    pipelineExecutionMetricsTracker.processorFailedOnUnexpectedError(pipeline.getId(), executionStep.getProcessorName(), doc, e);
                    throw new PipelineExecutionException(pipeline.getName(), e);
                }
            }
            logger.trace("pipeline {} executed successfully, took {}ms", pipeline.getId(), pipelineStopwatch.pipelineElapsed());
        }
        finally {
            pipelineStopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        pipelineExecutionMetricsTracker.processedDocSuccessfully(pipeline.getId(), doc, pipelineStopwatch.pipelineElapsed());

        return ExecutionResult.success();
    }

    private ProcessResult executeProcessor(Doc doc, Processor processor, PipelineStopwatch pipelineStopwatch, String pipelineId, String processorName) {
        pipelineStopwatch.reset();
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

    private void executeOnFailure(Doc doc, List<Processor> onFailureProcessors, PipelineStopwatch pipelineStopwatch, String pipelineId, String processorName) {
        for (Processor processor : onFailureProcessors) {
            executeProcessor(doc, processor, pipelineStopwatch, pipelineId, processorName + "-" + processor.getType());
        }
    }

    private class PipelineStopwatch {
        private Stopwatch stopwatch;
        private long timeElapsed;
        private TimeUnit timeUnit;

        public PipelineStopwatch(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        public PipelineStopwatch start() {
            stopwatch = Stopwatch.createStarted();
            timeElapsed = 0;
            return this;
        }

        public long pipelineElapsed() {
            return stopwatch.elapsed(timeUnit);
        }

        public long processorElapsed() {
            return stopwatch.elapsed(timeUnit) - timeElapsed;
        }

        public void reset() {
            timeElapsed = stopwatch.elapsed(timeUnit);
        }

        public void stop() {
            stopwatch.stop();
        }
    }
}
