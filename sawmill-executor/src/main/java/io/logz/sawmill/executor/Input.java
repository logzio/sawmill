package io.logz.sawmill.executor;

import io.logz.sawmill.Doc;

import java.util.List;
import java.util.Map;

public interface Input {
    List<Doc> listen();

    interface Factory {
        Input create(Map<String,Object> config);
    }

    interface Configuration {
    }
}
