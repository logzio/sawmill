package io.logz.sawmill.processes;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Process;
import io.logz.sawmill.annotations.ProcessProvider;
import io.logz.sawmill.utilities.JsonUtils;

public class TestProcess implements Process {
    public static final String NAME = "test";

    public final String value;

    public TestProcess(String value) {
        this.value = value;
    }

    @Override
    public void execute(Doc doc) {

    }

    @Override
    public String getName() { return NAME; }

    public String getValue() { return value; }

    @ProcessProvider(type = "test")
    public static class Factory implements Process.Factory {
        public Factory() {
        }

        @Override
        public Process create(String config) {
            Configuration testConfiguration = JsonUtils.fromJsonString(Configuration.class, config);

            return new TestProcess(testConfiguration.getValue());
        }
    }

    public static class Configuration implements Process.Configuration {
        private String value;

        public Configuration() { }

        public Configuration(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
