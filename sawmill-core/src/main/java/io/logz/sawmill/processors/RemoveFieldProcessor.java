package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

@ProcessorProvider(type = RemoveFieldProcessor.TYPE, factory = RemoveFieldProcessor.Factory.class)
public class RemoveFieldProcessor implements Processor {
    public static final String TYPE = "removeField";

    private final String path;

    public RemoveFieldProcessor(String path) {
        this.path = path;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ProcessResult process(Doc doc) {
        doc.removeField(path);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            RemoveFieldProcessor.Configuration removeFieldConfig = JsonUtils.fromJsonString(RemoveFieldProcessor.Configuration.class, config);

            return new RemoveFieldProcessor(removeFieldConfig.getPath());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;

        public Configuration() { }

        public Configuration(String path) {
            this.path = path;
        }

        public String getPath() { return path; }
    }
}
