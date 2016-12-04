package io.logz.sawmill;

import java.util.Map;

public interface Processor {
    ProcessResult process(Doc doc);

    interface Factory {
        Processor create(Map<String,Object> config);
    }

    interface Configuration {
    }
}
