package io.logz.sawmill;

public class WatchedPipeline {
    private final Doc doc;
    private final String pipelineId;
    private final long ingestTimestamp;
    private boolean notifiedAsOvertime;
    private Thread context;

    public WatchedPipeline(Doc doc, String pipelineId, long ingestTimestamp, Thread context) {
        this.doc = doc;
        this.pipelineId = pipelineId;
        this.ingestTimestamp = ingestTimestamp;
        this.notifiedAsOvertime = false;
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

    public Thread getContext() {
        return context;
    }

    public boolean hasBeenNotifiedAsOvertime() {
        return notifiedAsOvertime;
    }

    public void setAsNotifiedWithOvertime() {
        this.notifiedAsOvertime = true;
    }
}
