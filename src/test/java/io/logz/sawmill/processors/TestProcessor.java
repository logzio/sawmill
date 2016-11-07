package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.utilities.JsonUtils;

public class TestProcessor implements Processor {
    public static final String NAME = "test";

    public final String value;

    public TestProcessor(String value) {
        this.value = value;
    }

    @Override
    public void process(Doc doc) {

    }

    @Override
    public String getName() { return NAME; }

    public String getValue() { return value; }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            Configuration testConfiguration = JsonUtils.fromJsonString(Configuration.class, config);

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
