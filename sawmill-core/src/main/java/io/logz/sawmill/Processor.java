package io.logz.sawmill;

public interface Processor {
    ProcessResult process(Doc doc);

    interface Factory {
        Processor create(String config);
    }

    interface Configuration {
    }
}
