package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "json", factory = JsonProcessor.Factory.class)
public class JsonProcessor implements Processor {
    private final String field;
    private final Template targetField;

    public JsonProcessor(String field, Template targetField) {
        this.field = checkNotNull(field, "field cannot be null");
        this.targetField = targetField;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to parse json, couldn't find field [%s] or not instance of [%s]", field, String.class));
        }

        Map<String, Object> jsonMap;
        String jsonString = doc.getField(field);

        try {
            jsonMap = JsonUtils.fromJsonString(Map.class, jsonString);
        } catch (RuntimeException e) {
            doc.appendList("tags", "_jsonparsefailure");
            return ProcessResult.failure(String.format("failed to parse json, couldn't deserialize from json [%s]", jsonString));
        }

        doc.removeField(field);

        if (targetField != null) {
            doc.addField(targetField.render(doc), jsonMap);
        } else {
            jsonMap.forEach(doc::addField);
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
        public Processor create(Map<String,Object> config) {
            JsonProcessor.Configuration jsonConfig = JsonUtils.fromJsonMap(JsonProcessor.Configuration.class, config);

            Template targetField = StringUtils.isEmpty(jsonConfig.getTargetField()) ? null : templateService.createTemplate(jsonConfig.getTargetField());
            return new JsonProcessor(jsonConfig.getField(), targetField);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField;

        public Configuration() { }

        public Configuration(String field, String targetField) {
            this.field = field;
            this.targetField = targetField;
        }

        public String getField() { return field; }

        public String getTargetField() { return targetField; }
    }
}
