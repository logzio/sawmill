package io.logz.sawmill;

import org.apache.commons.collections4.queue.CircularFifoQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineExecutionTimeWatchdog {
    public static final int QUEUE_SIZE = 10;
    private final CircularFifoQueue<Bucket> circularFifoQueue;
    private final long timeFrame;
    private final Consumer<ExecutionContext> overtimeOp;

    public PipelineExecutionTimeWatchdog(long thresholdTimeMs, Consumer<ExecutionContext> overtimeOp) {
        this.overtimeOp = overtimeOp;
        this.timeFrame = thresholdTimeMs / QUEUE_SIZE;
        this.circularFifoQueue = new CircularFifoQueue<>(initQueue());
        initWatchdog();
    }

    private void initWatchdog() {
        ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
        timer.scheduleAtFixedRate(this::tick, 0, timeFrame, MILLISECONDS);
    }

    private List<Bucket> initQueue() {
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < QUEUE_SIZE; i++) buckets.add(new Bucket());
        return buckets;
    }

    public Bucket getCurrentBucket() {
        return circularFifoQueue.get(QUEUE_SIZE - 1);
    }

    private void tick() {
        Bucket bucket = circularFifoQueue.poll();
        alertOvertimeProcessingDocs(bucket);

        bucket.clearBucket();

        circularFifoQueue.add(bucket);
    }

    private void alertOvertimeProcessingDocs(Bucket bucket) {
        bucket.getOnProcessDocs().forEach(overtimeOp);
    }

    public class Bucket {
        private final ConcurrentMap<String, ExecutionContext> idToInfo;

        private Bucket() {
            idToInfo = new ConcurrentHashMap<>();
        }

        public void addDoc(String id, ExecutionContext context) {
            idToInfo.put(id, context);
        }

        public void removeDoc(String id) {
            idToInfo.remove(id);
        }

        private void clearBucket() {
            idToInfo.clear();
        }

        private List<ExecutionContext> getOnProcessDocs() {
            return idToInfo.values().stream().collect(Collectors.toList());
        }
    }
}
