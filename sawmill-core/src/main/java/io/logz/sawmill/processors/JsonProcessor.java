package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "json", factory = JsonProcessor.Factory.class)
public class JsonProcessor implements Processor {
    private final String field;
    private final String targetField;

    public JsonProcessor(String field, String targetField) {
        this.field = checkNotNull(field, "field cannot be null");
        this.targetField = targetField;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field)) {
            return ProcessResult.failure(String.format("failed to parse json, couldn't find field [%s]", field));
        }

        Map<String, Object> jsonMap;
        String jsonString = doc.getField(this.field);

        try {
            jsonMap = JsonUtils.fromJsonString(Map.class, jsonString);
        } catch (RuntimeException e) {
            return ProcessResult.failure(String.format("failed to parse json, couldn't deserialize from json [%s]", jsonString));
        }

        if (targetField != null) {
            doc.addField(targetField, jsonMap);
        } else {
            jsonMap.entrySet().forEach(entry -> {
                doc.addField(entry.getKey(), entry.getValue());
            });
        }
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(Map<String,Object> config) {
            JsonProcessor.Configuration jsonConfig = JsonUtils.fromJsonMap(JsonProcessor.Configuration.class, config);

            return new JsonProcessor(jsonConfig.getField(), jsonConfig.getTargetField());
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
