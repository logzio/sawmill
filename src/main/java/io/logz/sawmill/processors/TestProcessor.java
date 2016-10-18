package io.logz.sawmill.processors;

import io.logz.sawmill.Log;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

public class TestProcessor implements Processor {
    public static final String TYPE = "test";

    public final String value;

    public TestProcessor(String value) {
        this.value = value;
    }

    @Override
    public void execute(Log log) {

    }

    @Override
    public String getType() { return TYPE; }

    public String getValue() { return value; }

    @ProcessorProvider(type = "test")
    public static final class Factory implements Processor.Factory {
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

        public void setValue(String value) {
            this.value = value;
        }
    }
}
