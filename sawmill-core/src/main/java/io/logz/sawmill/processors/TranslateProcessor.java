package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;

import javax.inject.Inject;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "translate", factory = TranslateProcessor.Factory.class)
public class TranslateProcessor implements Processor {
    private final String field;
    private final String targetField;
    private final Map<String, String> dictionary;
    private final Template fallback;

    public TranslateProcessor(String field, String targetField, Map<String, String> dictionary, Template fallback) {
        this.field = requireNonNull(field, "field cannot be null");
        this.targetField = requireNonNull(targetField, "targetField cannot be null");
        this.dictionary = requireNonNull(dictionary, "dictionary cannot be null");
        this.fallback = fallback;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to translate field in path [%s], field is missing or not instance of String", field));
        }

        String value = doc.getField(this.field);

        String translation = dictionary.get(value);

        if (translation == null) {
            if (fallback == null) {
                return ProcessResult.failure(String.format("failed to translate field in path [%s], value=[%s] is not in dictionary", field, value));
            }

            doc.addField(targetField, fallback.render(doc));
        } else {
            doc.addField(targetField, translation);
        }

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        private final TemplateService templateService;

        @Inject
        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public TranslateProcessor create(Map<String,Object> config) {
            TranslateProcessor.Configuration translateConfig = JsonUtils.fromJsonMap(TranslateProcessor.Configuration.class, config);

            String field = translateConfig.getField();
            Map<String, String> dictionary = translateConfig.getDictionary();

            if (field == null || dictionary == null) {
                throw new ProcessorConfigurationException("field and dictionary cannot be null");
            }

            Template fallback = translateConfig.getFallback() != null ?
                    templateService.createTemplate(translateConfig.getFallback()) :
                    null;

            return new TranslateProcessor(field,
                    translateConfig.getTargetField(),
                    dictionary,
                    fallback);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField = "translation";
        private Map<String, String> dictionary;
        private String fallback;

        public Configuration() { }

        public String getField() {
            return field;
        }

        public String getTargetField() {
            return targetField;
        }

        public Map<String, String> getDictionary() {
            return dictionary;
        }

        public String getFallback() {
            return fallback;
        }
    }
}
