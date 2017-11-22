package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "size", factory = DocSizeProcessor.Factory.class)
public class DocSizeProcessor implements Processor {

    private final String charset;
    public DocSizeProcessor(String charSet){
        this.charset = checkNotNull(charSet, "Character set cannot be null");
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (doc.hasField("docSize")){
            return ProcessResult.failure("docSize field already exists");
        }
        String sourceAsJsonString = JsonUtils.toJsonString(doc.getSource());
        Charset charSetForName;
        try{
            charSetForName = Charset.forName(this.charset);
        } catch (UnsupportedCharsetException e){
            return ProcessResult.failure(String.format("Unsupported characterSet %s", charset));
        }
        doc.addField("docSize", charSetForName.encode(sourceAsJsonString).limit());
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {

        public Factory(){}

        @Override
        public DocSizeProcessor create(Map<String, Object> config) {
            DocSizeProcessor.Configuration processorConfig = JsonUtils.fromJsonMap(DocSizeProcessor.Configuration.class, config);
            return new DocSizeProcessor(processorConfig.getCharset());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String charset;
        public Configuration() { }

        public Configuration(String charset){
            this.charset = charset;
        }

        public String getCharset() {
            return charset;
        }
    }
}
