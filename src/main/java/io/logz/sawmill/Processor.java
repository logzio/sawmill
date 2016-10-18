package io.logz.sawmill;

public interface Processor {
    void execute(Log log);

    String getType();

    interface Factory {
        Processor create(String config);
    }

    interface Configuration {

    }
}
