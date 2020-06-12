package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.SawmillConfiguration;
import io.logz.sawmill.annotations.ProcessorProvider;

import javax.inject.Inject;
import java.util.Map;

@ProcessorProvider(type = "testProcessorWithDependencies", factory = TestProcessorWithDependencies.Factory.class)
public class TestProcessorWithDependencies implements Processor {

    @Override
    public ProcessResult process(Doc doc) {
        return ProcessResult.success();
    }

    public static class FactoryConfiguration implements SawmillConfiguration {
    }

    public static class Factory implements Processor.Factory {

        @Inject
        public Factory(FactoryConfiguration configuration) {
        }

        @Override
        public Processor create(Map<String, Object> config) {
            return new TestProcessorWithDependencies();
        }
    }
}
