package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutor {
    public static final int QUEUE_SIZE = 10;

    private final CircularFifoQueue<Bucket> circularFifoQueue;
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);
    private final long timeFrame;

    public PipelineExecutor(long thresholdTime) {
        this.timeFrame = thresholdTime / QUEUE_SIZE;
        this.circularFifoQueue = new CircularFifoQueue<>(initQueue());
        initWatchdog();
    }

    private List<Bucket> initQueue() {
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < QUEUE_SIZE; i++) buckets.add(new Bucket());
        return buckets;
    }

    private void initWatchdog() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                tick();
            }
        }, 0, timeFrame);
    }

    public void executePipeline(Pipeline pipeline, Doc doc) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        Bucket currentBucket = getCurrentBucket();
        String id = UUID.randomUUID().toString();
        currentBucket.addDoc(id, doc);

        try {
            for (Processor processor : pipeline.getProcessors()) {
                try {
                    processor.process(doc);
                    logger.info("processor {} executed successfully, took {}ms", processor.getName(), stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                    timeElapsed = stopwatch.elapsed(MILLISECONDS);
                } catch (Exception e) {
                    throw new PipelineExecutionException(String.format("failed to execute processor %s", processor.getName()), e);
                }
            }
            logger.info("pipeline executed successfully, took {}ms", stopwatch.elapsed(MILLISECONDS));
        } catch (PipelineExecutionException e) {
            logger.error("pipeline failed");
        }
        finally {
            stopwatch.stop();
            currentBucket.removeDoc(id);
        }
    }

    private Bucket getCurrentBucket() {
        return circularFifoQueue.get(QUEUE_SIZE - 1);
    }

    private void tick() {
        Bucket bucket = circularFifoQueue.peek();
        bucket.alertOvertimeProcessingLogs();

        bucket.clearBucket();

        circularFifoQueue.add(bucket);
    }

    private class Bucket {
        private final ConcurrentMap<String, Doc> docs;

        private Bucket() {
            docs = new ConcurrentHashMap<>();
        }

        private void addDoc(String id, Doc doc) {
            docs.put(id, doc);
        }

        private void removeDoc(String id) {
            docs.remove(id);
        }

        private void clearBucket() {
            docs.clear();
        }

        private void alertOvertimeProcessingLogs() {
            docs.values().stream().forEach(doc -> {
                logger.warn("processing {} takes too long, more than threshold={}", doc, timeFrame*QUEUE_SIZE);
            });
        }
    }
}
