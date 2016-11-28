package io.logz.sawmill;

public interface Processor {
    ProcessResult process(Doc doc);

    String getType();

    interface Factory {
        Processor create(String config);
    }

    interface Configuration {
    }
}
