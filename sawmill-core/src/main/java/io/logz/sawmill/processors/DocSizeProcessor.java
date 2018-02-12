package io.logz.sawmill.processors;

import com.google.common.base.Utf8;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "docSize", factory = DocSizeProcessor.Factory.class)
public class DocSizeProcessor implements Processor {
    private final String targetField;

    public DocSizeProcessor(String targetField){
        this.targetField = requireNonNull(targetField);
    }

    @Override
    public ProcessResult process(Doc doc) {
        String sourceAsJsonString = JsonUtils.toJsonString(doc.getSource());
        doc.addField(targetField, Utf8.encodedLength(sourceAsJsonString));
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {

        public Factory(){}

        @Override
        public DocSizeProcessor create(Map<String, Object> config) {
            DocSizeProcessor.Configuration processorConfig = JsonUtils.fromJsonMap(DocSizeProcessor.Configuration.class, config);

            return new DocSizeProcessor(processorConfig.getTargetField());
        }
    }

    public static class Configuration implements Processor.Configuration{
        private String targetField = "docSize";
        public Configuration(){}

        public Configuration(String targetField){
            this.targetField = targetField;
        }

        public String getTargetField() {
            return targetField;
        }
    }
}
