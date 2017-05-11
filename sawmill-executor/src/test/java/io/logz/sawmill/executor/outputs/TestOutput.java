package io.logz.sawmill.executor.outputs;

import io.logz.sawmill.Doc;
import io.logz.sawmill.executor.Output;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

public class TestOutput implements Output {
    private final String value;

    public TestOutput(String value) {
        this.value = value;
    }

    @Override
    public void send(List<Doc> docs) {

    }

    public String getValue() {
        return value;
    }

    public static class Factory implements Output.Factory {
        @Override
        public Output create(Map<String, Object> config) {
            Configuration testConfiguration = JsonUtils.fromJsonMap(Configuration.class, config);

            return new TestOutput(testConfiguration.getValue());
        }
    }

    public static class Configuration implements Output.Configuration {
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
