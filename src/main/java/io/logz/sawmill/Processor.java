package io.logz.sawmill;

import java.util.Map;

public interface Processor {
    void execute(Log log);

    String getType();

    interface Factory {
        Processor create(Map<String, Object> config);
    }

    interface Configuration {

    }
}
