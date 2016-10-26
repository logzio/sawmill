package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PipelineExecutor {
    public static final int NUMBER_OF_BUCKETS = 10;
    public static final int TIME_FRAME = 100;
    private final CircularFifoQueue<Bucket> circularFifoQueue;
    private final ExecutorService timeoutService;
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);


    public PipelineExecutor() {
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_BUCKETS; i++) buckets.add(new Bucket());
        this.circularFifoQueue = new CircularFifoQueue<>(buckets);
        this.timeoutService  = Executors.newFixedThreadPool(NUMBER_OF_BUCKETS);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                tick();
            }
        }, 0, TIME_FRAME);
    }

    public void executePipeline(Pipeline pipeline, Doc doc) {
        Bucket currentBucket = getCurrentBucket();
        String id = UUID.randomUUID().toString();
        currentBucket.addDoc(id, doc);

        try {
            doc.setProcessing();
            for (Process process : pipeline.getProcesses()) {
                try {
                    process.execute(doc);
                } catch (Exception e) {
                    throw new PipelineExecutionException(String.format("failed to execute process %s", process.getName()), e);
                }
            }
            doc.setSucceeded();
        } catch (PipelineExecutionException e) {
            doc.setFailed();
            logger.error("pipeline failed");
        }
        finally {
            currentBucket.removeDoc(id);
        }
    }

    private Bucket getCurrentBucket() {
        return circularFifoQueue.get(NUMBER_OF_BUCKETS - 1);
    }

    private void tick() {
        Bucket bucket = circularFifoQueue.peek();
        timeoutService.execute(() -> bucket.alertOvertimeProcessingLogs());

        circularFifoQueue.add(new Bucket());
    }

    private class Bucket {
        private Map<String, Doc> docs;

        private Bucket() {
            docs = new HashMap<>();
        }

        private void addDoc(String id, Doc doc) {
            docs.put(id, doc);
        }

        private void removeDoc(String id) {
            docs.remove(id);
        }

        private void alertOvertimeProcessingLogs() {
            docs.values().stream().forEach(doc -> {
                logger.warn("processing {} takes too long", doc);
            });
        }
    }
}
