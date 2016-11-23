package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

public class RemoveFieldProcessor implements Processor {
    private static final String TYPE = "removeField";

    private final String path;

    public RemoveFieldProcessor(String path) {
        this.path = path;
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    public ProcessResult process(Doc doc) {
        boolean succeeded = doc.removeField(path);
        return succeeded ? new ProcessResult(true) : new ProcessResult(false, String.format("failed to remove field in path [%s]", path));
    }

    @ProcessorProvider(name = TYPE)
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
