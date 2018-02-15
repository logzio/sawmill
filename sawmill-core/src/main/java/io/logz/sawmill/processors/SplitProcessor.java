package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Arrays;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "split", factory = SplitProcessor.Factory.class)
public class SplitProcessor implements Processor {
    private final String field;
    private final String separator;

    public SplitProcessor(String path, String separator) {
        this.field = requireNonNull(path, "field cannot be null");
        this.separator = requireNonNull(separator, "separator cannot be null");
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to split field in path [%s], field is missing or not instance of String", field));
        }

        String value = doc.getField(field);
        String[] split = value.split(separator);
        if (split.length > 1) {
            doc.addField(field, Arrays.asList(split));
        }

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public SplitProcessor create(Map<String,Object> config) {
            SplitProcessor.Configuration splitConfig = JsonUtils.fromJsonMap(SplitProcessor.Configuration.class, config);

            return new SplitProcessor(splitConfig.getField(), splitConfig.getSeparator());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String separator;

        public Configuration() { }

        public Configuration(String field, String separator) {
            this.field = field;
            this.separator = separator;
        }

        public String getField() {
            return field;
        }

        public String getSeparator() {
            return separator;
        }
    }
}
