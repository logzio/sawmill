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

                         if (!executionStep.getOnFailureProcessors().isPresent()) {
                             return ExecutionResult.failure(processResult.getError().get().getMessage(),
                                     executionStep.getProcessorName(),
                                     processResult.getError().get().getException().isPresent() ?
                                             new PipelineExecutionException(pipeline.getName(), processResult.getError().get().getException().get()) :
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
            logger.trace("pipeline executed successfully, took {}ms", pipelineStopwatch.elapsed(NANOSECONDS));
        }
        finally {
            pipelineStopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        pipelineExecutionMetricsTracker.processedDocSuccessfully(pipeline.getId(), doc, pipelineStopwatch.elapsed(NANOSECONDS));

        return ExecutionResult.success();
    }

    private ProcessResult executeProcessor(Doc doc, Processor processor, PipelineStopwatch pipelineStopwatch, String pipelineId, String processorName) {
        ProcessResult processResult = processor.process(doc);
        long totalProcessTime = pipelineStopwatch.elapsed(NANOSECONDS);
        long processorTook = totalProcessTime - pipelineStopwatch.getTimeElapsed();
        pipelineStopwatch.setTimeElapsed(totalProcessTime);

        if (processResult.isSucceeded()) {
            logger.trace("processor {} executed successfully, took {}ns", processorName, processorTook);
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

        public void setTimeElapsed(long timeElapsed) {
            this.timeElapsed = timeElapsed;
        }

        public long getTimeElapsed() {
            return timeElapsed;
        }

        public PipelineStopwatch start() {
            stopwatch = Stopwatch.createStarted();
            timeElapsed = 0;
            return this;
        }

        public long elapsed(TimeUnit timeUnit) {
            return stopwatch.elapsed(timeUnit);
        }

        public void stop() {
            stopwatch.stop();
        }
    }
}
