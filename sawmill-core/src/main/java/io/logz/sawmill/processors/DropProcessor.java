package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;
import java.util.Random;

@ProcessorProvider(type = "drop", factory = DropProcessor.Factory.class)
public class DropProcessor implements Processor {
    private final int percentage;
    private final Random random;

    public DropProcessor(int percentage) {
        this.percentage = percentage;
        this.random = new Random();
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (random.nextFloat() > percentage / 100.0) {
            return ProcessResult.success();
        }

        return ProcessResult.drop();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public DropProcessor create(Map<String,Object> config) {
            DropProcessor.Configuration dropConfig = JsonUtils.fromJsonMap(DropProcessor.Configuration.class, config);

            return new DropProcessor(dropConfig.getPercentage());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private int percentage = 100;

        public Configuration() { }

        public int getPercentage() {
            return percentage;
        }
    }
}
