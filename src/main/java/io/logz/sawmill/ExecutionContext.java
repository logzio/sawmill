package io.logz.sawmill;

import java.util.Date;

public class ExecutionContext {
    private final Doc doc;
    private final String pipelineId;
    private final Date ingestTimestamp;

    public ExecutionContext(Doc doc, String pipelineId, Date ingestTimestamp) {
        this.doc = doc;
        this.pipelineId = pipelineId;
        this.ingestTimestamp = ingestTimestamp;
    }

    public Doc getDoc() { return doc; }

    public String getPipelineId() { return pipelineId; }

    public Date getIngestTimestamp() { return ingestTimestamp; }
}
