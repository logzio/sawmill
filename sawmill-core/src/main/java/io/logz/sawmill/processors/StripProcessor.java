package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

@ProcessorProvider(type = "strip", factory = StripProcessor.Factory.class)
public class StripProcessor implements Processor {
    private final List<String> fields;

    public StripProcessor(List<String> fields) {
        checkState(CollectionUtils.isNotEmpty(fields), "fields cannot be empty");
        this.fields = fields;
    }

    @Override
    public ProcessResult process(Doc doc) {
        List<String> failedFields = new ArrayList<>();
        for (String field : fields) {
            if (!doc.hasField(field, String.class)) {
                failedFields.add(field);
                continue;
            }

            String value = doc.getField(field);
            doc.addField(field, value.trim());

        }

        if (CollectionUtils.isNotEmpty(failedFields)) {
            return ProcessResult.failure(String.format("failed to strip the following fields [%s], fields are missing or not instance of String", failedFields));
        }

        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public StripProcessor create(Map<String,Object> config) {
            StripProcessor.Configuration stripConfig = JsonUtils.fromJsonMap(StripProcessor.Configuration.class, config);

            return new StripProcessor(stripConfig.getFields());
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
