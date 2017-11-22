package io.logz.sawmill.processors;

import com.google.common.base.Utf8;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import java.util.Map;

@ProcessorProvider(type = "size", factory = DocSizeProcessor.Factory.class)
public class DocSizeProcessor implements Processor {

    public DocSizeProcessor(){}

    @Override
    public ProcessResult process(Doc doc) {
        if (doc.hasField("docSize")){
            return ProcessResult.failure("docSize field already exists");
        }
        String sourceAsJsonString = JsonUtils.toJsonString(doc.getSource());
        doc.addField("docSize", Utf8.encodedLength(sourceAsJsonString));
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {

        public Factory(){}

        @Override
        public DocSizeProcessor create(Map<String, Object> config) { return new DocSizeProcessor();
        }
    }
}
