package io.logz.sawmill;

public interface Processor {
    void process(Doc doc);

    String getName();

    interface Factory {
        Processor create(String config);
    }

    interface Configuration {

    }
}
