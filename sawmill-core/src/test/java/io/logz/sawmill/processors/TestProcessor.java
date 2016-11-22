package io.logz.sawmill.processors;

import io.logz.sawmill.AbstractProcessor;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.ArrayList;
import java.util.List;

public class TestProcessor implements Processor {
    public static final String NAME = "test";

    public final String value;
    private final List<Processor> onFailureProcessors;
    private final boolean ignoreFailure;

    public TestProcessor(String value, List<Processor> onFailureProcessors, boolean ignoreFailure) {
        this.onFailureProcessors = onFailureProcessors;
        this.ignoreFailure = ignoreFailure;
        this.value = value;
    }

    @Override
    public void process(Doc doc) {

    }

    @Override
    public String getName() { return NAME; }

    public String getValue() { return value; }

    public List<Processor> getOnFailureProcessors() {
        return onFailureProcessors;
    }

    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    public static class Factory extends AbstractProcessor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config, ProcessorFactoryRegistry processorFactoryRegistry) {
            Configuration testConfiguration = JsonUtils.fromJsonString(Configuration.class, config);

            List<Processor> onFailureProcessors = extractProcessors(testConfiguration.getOnFailureProcessors(), processorFactoryRegistry);

            return new TestProcessor(testConfiguration.getValue(), onFailureProcessors, testConfiguration.isIgnoreFailure());
        }
    }

    public static class Configuration extends AbstractProcessor.Configuration {
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
