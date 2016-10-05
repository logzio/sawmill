package io.logz.sawmill;

import io.logz.sawmill.utilities.JsonUtils;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Log {

    private final Map<String, Object> source;
    private final Map<String, Object> metadata;

    public Log(Map<String, Object> source, Map<String, Object> metadata) {
        this.source = source;
        this.metadata = metadata;
    }

    public Log(JSONObject json) {
        if (json == null || json.isEmpty()) throw new IllegalArgumentException("source cannot be empty");
        this.source = JsonUtils.fromJsonString(Map.class, json.toJSONString());
        this.metadata = new HashMap<>();
    }

    public Map<String, Object> getSource() { return source; }

    public Map<String, Object> getMetadata() { return metadata; }

    public <T> T getFieldValue(String path, Class<T> type) {
        Optional<Object> field = JsonUtils.getByPath(source, path);
        if (!field.isPresent()) throw new IllegalArgumentException(String.format("Couldn't resolve field in path [%s]"));
        return cast(field.get(), type);
    }

    public <T> void addFieldValue(String k, T v) {
        source.put(k, v);
    }

    private <T> T cast(Object object, Class<T> type) {
        if (type.isInstance(object)) {
            return type.cast(object);
        }
        throw new IllegalArgumentException(String.format("Couldn't cast object type [%s] to type [%s]", object.getClass().getName(), type.getName()));
    }

    @Override
    public String toString() {
        return "Log{" +
                "source=" + source +
                ", metadata=" + metadata +
                '}';
    }
}
