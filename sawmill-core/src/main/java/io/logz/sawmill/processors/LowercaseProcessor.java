package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "lowerCase", factory = LowerCaseProcessor.Factory.class)
public class LowerCaseProcessor implements Processor {
    private final String field;

    public LowerCaseProcessor(String path) {
        this.field = checkNotNull(path, "field cannot be null");
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to lowercase field in path [%s], field is missing or not instance of [%s]", field, String.class));
        }

        String value = doc.getField(field);
        doc.addField(field, value.toLowerCase());

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public LowerCaseProcessor create(Map<String,Object> config) {
            LowerCaseProcessor.Configuration lowerCaseConfig = JsonUtils.fromJsonMap(LowerCaseProcessor.Configuration.class, config);

            return new LowerCaseProcessor(lowerCaseConfig.getField());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;

        public Configuration() { }

        public Configuration(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }
    }
}
