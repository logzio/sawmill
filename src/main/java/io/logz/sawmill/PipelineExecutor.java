package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class PipelineExecutor {
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);

    private final PipelineExecutionTimeWatchdog watchdog;
    private final PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker;

    public PipelineExecutor() {
        this(new SawmillConfig());
    }

    public PipelineExecutor(SawmillConfig config) {
        this(config.getWatchdog(), config.getMetricsTracker());
    }

    private PipelineExecutor(PipelineExecutionTimeWatchdog watchdog, PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker) {
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
                } catch (Exception e) {
                    pipelineExecutionMetricsTracker.processorFailed(pipeline.getId(), processor.getName(), doc);
                    throw new PipelineExecutionException(pipeline.getName(), processor.getName(), e);
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
