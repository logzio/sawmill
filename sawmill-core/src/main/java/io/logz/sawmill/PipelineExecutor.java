package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        long executionIdentifier = watchdog.startedExecution(new ExecutionContext(doc, pipeline.getId(), System.currentTimeMillis()));

        try {
            for (ExecutionStep executionStep : pipeline.getExecutionSteps()) {
                try {
                    Processor processor = executionStep.getProcessor();

                    ProcessResult processResult = processor.process(doc);

                    long totalProcessTime = stopwatch.elapsed(NANOSECONDS);
                    long processorTook = totalProcessTime - timeElapsed;
                    timeElapsed = totalProcessTime;

                     if (processResult.isSucceeded()) {
                         logger.trace("processor {} executed successfully, took {}ns", executionStep.getProcessorName(), processorTook);
                         pipelineExecutionMetricsTracker.processorFinished(pipeline.getId(), executionStep.getProcessorName(), processorTook);
                     } else {
                         pipelineExecutionMetricsTracker.processorFailed(pipeline.getId(), executionStep.getProcessorName(), doc);

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
                             executeOnFailure(doc, executionStep.getOnFailureProcessors().get());
                         }
                     }
                } catch (RuntimeException e) {
                    pipelineExecutionMetricsTracker.processorFailedOnUnexpectedError(pipeline.getId(), executionStep.getProcessorName(), doc, e);
                    return ExecutionResult.failure(String.format("failed to execute pipeline [%s] , processor [%s] thrown unexpected error", pipeline.getName(), executionStep.getProcessorName()),
                            executionStep.getProcessorName(),
                            new PipelineExecutionException(pipeline.getName(), e));
                }
            }
            logger.trace("pipeline executed successfully, took {}ms", stopwatch.elapsed(NANOSECONDS));
        }
        finally {
            stopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        pipelineExecutionMetricsTracker.processedDocSuccessfully(pipeline.getId(), doc, stopwatch.elapsed(NANOSECONDS));

        return ExecutionResult.success();
    }

    private void executeOnFailure(Doc doc, List<Processor> onFailureProcessors) {
        for (Processor processor : onFailureProcessors) {
            processor.process(doc);
        }
    }
}
