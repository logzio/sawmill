package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.DocumentBuilderProvider;
import io.logz.sawmill.utilities.JsonUtils;
import io.logz.sawmill.utilities.XPathExpressionProvider;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "xml", factory = XmlProcessor.Factory.class)
public class XmlProcessor implements Processor {
    private static final Logger logger = LoggerFactory.getLogger(XmlProcessor.class);

    private final DocumentBuilderProvider documentBuilderProvider;
    private final String field;
    private final String targetField;
    private final Map<XPathExpressionProvider, String> xpath;
    private final boolean storeXml;

    public XmlProcessor(DocumentBuilderProvider documentBuilderProvider, String field, String targetField, Map<XPathExpressionProvider, String> xpath, boolean storeXml) {
        this.documentBuilderProvider = requireNonNull(documentBuilderProvider);
        this.field = requireNonNull(field);
        this.targetField = targetField;
        this.xpath = xpath;
        this.storeXml = storeXml;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure("failed to parse xml in path [" + field + "], field is missing or not instance of String");
        }

        String value = doc.getField(this.field);
        Document parsed;

        try {
            InputStream inputStream = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
            parsed = documentBuilderProvider.provide().parse(inputStream);
        } catch (SAXException | IOException e) {
            return ProcessResult.failure("failed to parse xml in path [" + field + "] with value [" + value + "], errorMsg=[" + e.getMessage() + "]");
        }

        if (MapUtils.isNotEmpty(xpath)) {
            for (Map.Entry<XPathExpressionProvider, String> item : xpath.entrySet()) {
                try {
                    Object evaluate;
                    NodeList nodeList = (NodeList) item.getKey().provide().evaluate(parsed, XPathConstants.NODESET);
                    if (nodeList.getLength() == 0) continue;
                    if (nodeList.getLength() == 1) {
                        evaluate = nodeList.item(0).getTextContent();
                    } else {
                        evaluate = new ArrayList<>();
                        for (int i = 0; i < nodeList.getLength(); i++) {
                            ((List)evaluate).add(nodeList.item(i).getTextContent());
                        }
                    }

                    String path = item.getValue();
                    if (doc.hasField(path)) {
                        if (evaluate instanceof List) {
                            ((List)evaluate).forEach(val -> doc.appendList(path, val));
                        } else {
                            doc.appendList(path, evaluate);
                        }
                    } else {
                        doc.addField(path, evaluate);
                    }
                } catch (XPathExpressionException e) {
                    logger.trace("xpath evaluation failed", e);
                }
            }
        }

        if (storeXml) {
            Map<String, Object> xmlNodes = extractNodes(parsed);
            if (StringUtils.isNotEmpty(targetField)) {
                doc.addField(targetField, xmlNodes);
            } else {
                xmlNodes.forEach(doc::addField);
            }
        }
        return ProcessResult.success();
    }

    private Map<String, Object> extractNodes(Node parent) {
        Map<String, Object> xmlNodes = new HashMap<>();
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);

            String key = node.getNodeName();
            Object value;

            if (node.getChildNodes().getLength() == 1
                    && node.getChildNodes().item(0) != null
                    && node.getChildNodes().item(0).getNodeValue() != null ) {
                value = node.getChildNodes().item(0).getNodeValue();
            } else {
                value = extractNodes(node);
            }

            xmlNodes.compute(key, (k, oldVal) -> {
                if (node.getChildNodes().item(0) == null) { return oldVal; }
                if (oldVal == null) return value;
                if (oldVal instanceof List) {
                    ((List) oldVal).add(value);
                    return oldVal;
                }
                return new ArrayList<>(Arrays.asList(oldVal, value));
            });
        }

        return xmlNodes;
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public XmlProcessor create(Map<String,Object> config) {
            XmlProcessor.Configuration xmlConfig = JsonUtils.fromJsonMap(XmlProcessor.Configuration.class, config);

            DocumentBuilderProvider documentBuilderProvider = new DocumentBuilderProvider();

            Map<XPathExpressionProvider, String> xpath = null;

            if (MapUtils.isNotEmpty(xmlConfig.getXpath())) {
                xpath = xmlConfig.getXpath()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(e -> new XPathExpressionProvider(e.getKey()),
                                Map.Entry::getValue));

                // Initiate to prevent invalid expression
                xpath.keySet().forEach(XPathExpressionProvider::provide);
            }

            return new XmlProcessor(documentBuilderProvider,
                    xmlConfig.getField(),
                    xmlConfig.getTargetField(),
                    xpath,
                    xmlConfig.isStoreXml());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField;
        private Map<String, String> xpath;
        private boolean storeXml = true;

        public Configuration() { }

        public String getField() {
            return field;
        }

        public String getTargetField() {
            return targetField;
        }

        public Map<String, String> getXpath() {
            return xpath;
        }

        public boolean isStoreXml() {
            return storeXml;
        }
    }
}
