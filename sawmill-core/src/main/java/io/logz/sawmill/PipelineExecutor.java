package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.logz.sawmill.Pipeline.FailureHandler.ABORT;
import static io.logz.sawmill.Pipeline.FailureHandler.DROP;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class PipelineExecutor {
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);

    private final PipelineExecutionTimeWatchdog watchdog;
    private final PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker;

    public PipelineExecutor(PipelineExecutionTimeWatchdog watchdog, PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker) {
        this.watchdog = watchdog;
        this.pipelineExecutionMetricsTracker = pipelineExecutionMetricsTracker;
    }


    public void execute(Pipeline pipeline, Doc doc) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        long executionIdentifier = watchdog.startedExecution(new ExecutionContext(doc, pipeline.getId(), System.currentTimeMillis()));

        try {
            for (Processor processor : pipeline.getProcessors()) {
                try {
                    processor.process(doc);
                    long totalProcessTime = stopwatch.elapsed(NANOSECONDS);
                    long processorTook = totalProcessTime - timeElapsed;
                    timeElapsed = totalProcessTime;

                    logger.trace("processor {} executed successfully, took {}ns", processor.getName(), processorTook);
                    pipelineExecutionMetricsTracker.processorFinished(processor.getName(), processorTook);
                } catch (ProcessorExecutionException e) {
                    if (pipeline.getFailureHandler() == ABORT) {
                        pipelineExecutionMetricsTracker.processorFailed(pipeline.getId(), processor.getName(), doc);
                        throw new PipelineExecutionException(pipeline.getName(), e);
                    } else if (pipeline.getFailureHandler() == DROP) {
                        pipelineExecutionMetricsTracker.docDropped(doc);
                        break;
                    }
                } catch (Exception e) {
                    pipelineExecutionMetricsTracker.processorFailedOnUnexpectedError(pipeline.getId(), processor.getName(), doc, e);
                    throw new RuntimeException(String.format("failed to execute pipeline [%s] , processor [%s] thrown unexpected error", pipeline.getName(), processor.getName()), e);
                }
            }
            logger.trace("pipeline executed successfully, took {}ms", stopwatch.elapsed(NANOSECONDS));
        }
        finally {
            stopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        pipelineExecutionMetricsTracker.processedDocSuccessfully(pipeline.getId(), doc, stopwatch.elapsed(NANOSECONDS));
    }
}
