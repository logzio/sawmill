package io.logz.sawmill.http;

import java.util.Map;

public class ExternalMappingResponse {
    private final Long lastModified;
    private final Map<String, Iterable<String>> mappings;

    public ExternalMappingResponse(Long lastModified, Map<String, Iterable<String>> mappings) {
        this.lastModified = lastModified;
        this.mappings = mappings;
    }

    public boolean isModified() {
        return mappings != null;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public Map<String, Iterable<String>> getMappings() {
        return mappings;
    }
}
