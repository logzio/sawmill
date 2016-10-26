package io.logz.sawmill;

public interface Process {
    void execute(Doc doc);

    String getName();

    interface Factory {
        Process create(String config);
    }

    interface Configuration {

    }
}
