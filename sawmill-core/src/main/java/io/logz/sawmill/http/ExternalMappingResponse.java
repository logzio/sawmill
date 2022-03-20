package io.logz.sawmill.http;

import java.util.Map;

public class ExternalMappingResponse {
    private final Boolean modified;
    private final Long lastModified;
    private final Map<String, Iterable<String>> mappings;

    public ExternalMappingResponse(Boolean modified, Long lastModified, Map<String, Iterable<String>> mappings) {
        this.modified = modified;
        this.lastModified = lastModified;
        this.mappings = mappings;
    }

    public Boolean isModified() {
        return modified;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public Map<String, Iterable<String>> getMappings() {
        return mappings;
    }
}
