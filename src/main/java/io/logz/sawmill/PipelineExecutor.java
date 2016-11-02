package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutor {
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);

    private final PipelineExecutionTimeWatchdog watchdog;

    public PipelineExecutor(long thresholdTimeMs, Consumer<ExecutionContext> overtimeOp) {
        this.watchdog = new PipelineExecutionTimeWatchdog(thresholdTimeMs, overtimeOp);
    }


    public void executePipeline(Pipeline pipeline, Doc doc) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        PipelineExecutionTimeWatchdog.Bucket currentBucket = watchdog.getCurrentBucket();
        String id = UUID.randomUUID().toString();
        currentBucket.addDoc(id, new ExecutionContext(doc, pipeline.getId(), new Date()));

        try {
            for (Processor processor : pipeline.getProcessors()) {
                try {
                    processor.process(doc);
                    logger.trace("processor {} executed successfully, took {}ms", processor.getName(), stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                    timeElapsed = stopwatch.elapsed(MILLISECONDS);
                } catch (Exception e) {
                    throw new PipelineExecutionException(pipeline.getName(), processor.getName(), e);
                }
            }
            logger.trace("pipeline executed successfully, took {}ms", stopwatch.elapsed(MILLISECONDS));
        } catch (PipelineExecutionException e) {
            throw e;
        }
        finally {
            stopwatch.stop();
            currentBucket.removeDoc(id);
        }
    }
}
