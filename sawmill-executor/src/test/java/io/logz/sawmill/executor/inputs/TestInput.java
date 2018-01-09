package io.logz.sawmill.executor.inputs;

import io.logz.sawmill.Doc;
import io.logz.sawmill.executor.Input;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

public class TestInput implements Input {
    private final String value;

    public TestInput(String value) {
        this.value = value;
    }

    @Override
    public List<Doc> listen() {
        return null;
    }

    public String getValue() {
        return value;
    }

    public static class Factory implements Input.Factory {
        @Override
        public Input create(Map<String, Object> config) {
            Configuration testConfiguration = JsonUtils.fromJsonMap(Configuration.class, config);

            return new TestInput(testConfiguration.getValue());
        }
    }

    public static class Configuration implements Input.Configuration {
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
