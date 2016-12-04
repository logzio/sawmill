package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

public class TestProcessor implements Processor {
    public final String value;

    public TestProcessor(String value) {
        this.value = value;
    }

    @Override
    public ProcessResult process(Doc doc) {
        return null;
    }

    public String getValue() { return value; }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(Map<String,Object> config) {
            Configuration testConfiguration = JsonUtils.fromJsonMap(Configuration.class, config);

            return new TestProcessor(testConfiguration.getValue());
        }
    }

    public static class Configuration implements Processor.Configuration {
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
