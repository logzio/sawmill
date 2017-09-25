package io.logz.sawmill;

import java.util.concurrent.atomic.AtomicBoolean;

public class WatchedPipeline {
    private final Doc doc;
    private final String pipelineId;
    private final long ingestTimestamp;
    private boolean notifiedAsOvertime;
    private AtomicBoolean running;
    private final Thread context;

    public WatchedPipeline(Doc doc, String pipelineId, long ingestTimestamp, Thread context) {
        this.doc = doc;
        this.pipelineId = pipelineId;
        this.ingestTimestamp = ingestTimestamp;
        this.notifiedAsOvertime = false;
        this.running = new AtomicBoolean(true);
        this.context = context;
    }

    public Doc getDoc() {
        return doc;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public long getIngestTimestamp() {
        return ingestTimestamp;
    }

    public boolean hasBeenNotifiedAsOvertime() {
        return notifiedAsOvertime;
    }

    public void setAsNotifiedWithOvertime() {
        this.notifiedAsOvertime = true;
    }

    public boolean compareAndSetFinished() {
        return !running.compareAndSet(true, false);
    }

    public void interrupt() {
        context.interrupt();
    }
}
