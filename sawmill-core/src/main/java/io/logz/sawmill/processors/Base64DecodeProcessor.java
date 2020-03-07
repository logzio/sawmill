package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.codec.binary.Base64;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "base64Decode", factory = Base64DecodeProcessor.Factory.class)
public class Base64DecodeProcessor implements Processor {
    private final List<String> fields;

    public Base64DecodeProcessor(List<String> fields) {
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
            doc.addField(field, new String(Base64.decodeBase64(value)));
        }

        if (!missingFields.isEmpty()) {
            return ProcessResult.failure(String.format("failed to decode fields in path [%s], fields are missing or not instance of [%s]", missingFields, String.class));
        }

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Base64DecodeProcessor create(Map<String,Object> config) {
            Base64DecodeProcessor.Configuration base64Config = JsonUtils.fromJsonMap(Base64DecodeProcessor.Configuration.class, config);

            if (CollectionUtils.isEmpty(base64Config.getFields())) {
                throw new ProcessorConfigurationException("failed to parse base64Decode processor config, could not resolve fields");
            }

            return new Base64DecodeProcessor(base64Config.getFields());
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
