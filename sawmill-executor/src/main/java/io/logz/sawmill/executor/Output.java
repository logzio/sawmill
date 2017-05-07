package io.logz.sawmill.executor;

import io.logz.sawmill.Doc;

import java.util.List;
import java.util.Map;

public interface Output {
    void send(List<Doc> docs);

    interface Factory {
        Output create(Map<String,Object> config);
    }

    interface Configuration {
    }
}
