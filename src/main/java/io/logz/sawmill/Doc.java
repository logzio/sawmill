package io.logz.sawmill;

import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static io.logz.sawmill.Doc.State.FAILED;
import static io.logz.sawmill.Doc.State.PROCESSING;
import static io.logz.sawmill.Doc.State.RAW;
import static io.logz.sawmill.Doc.State.SUCCEEDED;

import static com.google.common.base.Preconditions.checkState;

public class Doc {

    private final Map<String, Object> source;
    private final Map<String, Object> metadata;

    private State state = RAW;

    public Doc(Map<String, Object> source, Map<String, Object> metadata) {
        checkState(!MapUtils.isEmpty(source), "source cannot be empty");
        this.source = source;
        this.metadata = metadata;

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ", Locale.ROOT);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.metadata.put("ingestTimestamp", df.format(new Date()));
    }

    public Doc(Map<String, Object> source) {
        this(source, new HashMap<>());
    }

    public Map<String, Object> getSource() { return source; }

    public Map<String, Object> getMetadata() { return metadata; }

    public <T> T getFieldValue(String path) {
        Optional<Object> field = JsonUtils.getByPath(source, path);
        checkState(field.isPresent(), String.format("Couldn't resolve field in path [%s]", path));
        return (T) field.get();
    }

    public State getState() { return state; }

    public void setProcessing() {
        state = PROCESSING;
    }

    public void setSucceeded() {
        state = SUCCEEDED;
    }

    public void setFailed() { state = FAILED; }

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

    public enum State {
        RAW,
        PROCESSING,
        SUCCEEDED,
        FAILED
    }
}
