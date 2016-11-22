package io.logz.sawmill;

public class ExecutionContext {
    private final Doc doc;
    private final String pipelineId;
    private final long ingestTimestamp;

    public ExecutionContext(Doc doc, String pipelineId, long ingestTimestamp) {
        this.doc = doc;
        this.pipelineId = pipelineId;
        this.ingestTimestamp = ingestTimestamp;
    }

    public Doc getDoc() { return doc; }

    public String getPipelineId() { return pipelineId; }

    public long getIngestTimestamp() { return ingestTimestamp; }
}
