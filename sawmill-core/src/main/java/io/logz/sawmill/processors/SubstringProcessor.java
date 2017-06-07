package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "substring", factory = SubstringProcessor.Factory.class)
public class SubstringProcessor implements Processor {
    private final String field;
    private final Integer begin;
    private final Integer end;

    public SubstringProcessor(String field, Integer begin, Integer end) {
        this.field = requireNonNull(field);
        this.begin = requireNonNull(begin);
        this.end = end;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to substring field [%s], field is missing or not instance of String", field));
        }

        String value = doc.getField(field);

        if (value.length() <= begin) {
            return ProcessResult.failure(String.format("failed to substring field [%s], value [%s] is shorter than beginIndex [%s]", field, value, begin));
        }

        String substring;
        if (end != null && value.length() >= end) {
            substring = value.substring(begin, end);
        } else {
             substring = value.substring(begin);
        }

        doc.addField(field, substring);

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public SubstringProcessor create(Map<String,Object> config) {
            SubstringProcessor.Configuration substringConfig = JsonUtils.fromJsonMap(SubstringProcessor.Configuration.class, config);

            return new SubstringProcessor(substringConfig.getField(), substringConfig.getBegin(), substringConfig.getEnd());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private Integer begin = 0;
        private Integer end;

        public Configuration() { }

        public String getField() {
            return field;
        }

        public Integer getBegin() {
            return begin;
        }

        public Integer getEnd() {
            return end;
        }
    }
}
