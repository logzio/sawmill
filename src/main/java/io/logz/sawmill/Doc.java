package io.logz.sawmill;

import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Doc {

    private final Map<String, Object> source;
    private final Map<String, Object> metadata;

    public Doc(Map<String, Object> source, Map<String, Object> metadata) {
        checkArgument(MapUtils.isEmpty(source), "source cannot be empty");
        this.source = source;
        this.metadata = metadata;
    }

    private void checkArgument(boolean notValid, String errorMsg) {
        if (notValid) throw new IllegalArgumentException(errorMsg);
    }

    public Doc(Map<String, Object> source) {
        this(source, new HashMap<>());
    }

    public Map<String, Object> getSource() { return source; }

    public Map<String, Object> getMetadata() { return metadata; }

    public <T> T getFieldValue(String path) {
        Optional<Object> field = JsonUtils.getByPath(source, path);
        checkArgument(!field.isPresent(), String.format("Couldn't resolve field in path [%s]"));
        return (T) field.get();
    }

    public <T> void addFieldValue(String k, T v) {
        source.put(k, v);
    }

    @Override
    public String toString() {
        return "Doc{" +
                "source=" + source +
                ", metadata=" + metadata +
                '}';
    }
}
