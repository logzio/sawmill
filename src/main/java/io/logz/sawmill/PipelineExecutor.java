package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutor {
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);

    private final PipelineExecutionTimeWatchdog watchdog;

    public PipelineExecutor(PipelineExecutionTimeWatchdog watchdog) {
        this.watchdog = watchdog;
    }


    public void execute(Pipeline pipeline, Doc doc) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        long executionIdentifier = watchdog.startedExecution(new ExecutionContext(doc, pipeline.getId(), System.currentTimeMillis()));

        try {
            for (Processor processor : pipeline.getProcessors()) {
                try {
                    processor.process(doc);
                    long totalProcessTime = stopwatch.elapsed(MILLISECONDS);
                    long processorTook = totalProcessTime - timeElapsed;
                    timeElapsed = totalProcessTime;

                    logger.trace("processor {} executed successfully, took {}ms", processor.getName(), processorTook);
                } catch (Exception e) {
                    throw new PipelineExecutionException(pipeline.getName(), processor.getName(), e);
                }
            }
            logger.trace("pipeline executed successfully, took {}ms", stopwatch.elapsed(MILLISECONDS));
        }
        finally {
            stopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }
    }
}
