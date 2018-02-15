package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "upperCase", factory = UpperCaseProcessor.Factory.class)
public class UpperCaseProcessor implements Processor {
    private final List<String> fields;

    public UpperCaseProcessor(List<String> fields) {
        this.fields = requireNonNull(fields, "fields cannot be null");
    }

    @Override
    public ProcessResult process(Doc doc) {
        List<String> missingFields = new ArrayList<>();
        for (String field : fields) {
            if (!doc.hasField(field, String.class)) {
                missingFields.add(field);
                continue;
            }

            String value = doc.getField(field);
            doc.addField(field, value.toUpperCase());
        }

        if (!missingFields.isEmpty()) {
            return ProcessResult.failure(String.format("failed to lowercase fields in path [%s], fields are missing or not instance of [%s]", missingFields, String.class));
        }

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public UpperCaseProcessor create(Map<String,Object> config) {
            UpperCaseProcessor.Configuration upperCaseConfig = JsonUtils.fromJsonMap(UpperCaseProcessor.Configuration.class, config);

            if (CollectionUtils.isEmpty(upperCaseConfig.getFields())) {
                throw new ProcessorConfigurationException("failed to parse upperCase processor config, could not resolve fields");
            }

            return new UpperCaseProcessor(upperCaseConfig.getFields());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private List<String> fields;

        public Configuration() { }

        public List<String> getFields() {
            return fields;
        }
    }
}
