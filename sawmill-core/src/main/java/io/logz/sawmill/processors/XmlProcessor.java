package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.DocumentBuilderProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@ProcessorProvider(type = "xml", factory = XmlProcessor.Factory.class)
public class XmlProcessor implements Processor {
    private final String field;
    private final DocumentBuilderProvider documentBuilderProvider;

    public XmlProcessor(String field, DocumentBuilderProvider documentBuilderProvider) {
        this.field = field;
        this.documentBuilderProvider = documentBuilderProvider;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to parse xml in path [%s], field is missing or not instance of String", field));
        }

        String value = doc.getField(this.field);

        try {
            InputStream inputStream = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
            Document parsed = documentBuilderProvider.provide().parse(inputStream);
        } catch (SAXException | IOException e) {
            return ProcessResult.failure(String.format("failed to parse xml in path [%s] with value [%s], errorMsg=[%s]", field, value, e.getMessage()));
        }
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public XmlProcessor create(Map<String,Object> config) {
            XmlProcessor.Configuration xmlConfig = JsonUtils.fromJsonMap(XmlProcessor.Configuration.class, config);

            DocumentBuilderProvider documentBuilderProvider = new DocumentBuilderProvider();

            return new XmlProcessor(xmlConfig.getField(), documentBuilderProvider);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;

        public Configuration() { }

        public String getField() {
            return field;
        }
    }
}
