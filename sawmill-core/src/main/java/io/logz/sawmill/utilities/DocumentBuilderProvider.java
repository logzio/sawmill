package io.logz.sawmill.utilities;

import io.logz.sawmill.exceptions.ProcessorConfigurationException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class DocumentBuilderProvider {

    private static final String DISALLOW_DOCTYPE = "http://apache.org/xml/features/disallow-doctype-decl";
    private static final String EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
    private static final String EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
    private static final String LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

    private final ThreadLocal<DocumentBuilder> localDocumentBuilder;

    public DocumentBuilderProvider() {
        localDocumentBuilder = ThreadLocal.withInitial(() -> {
            try {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setFeature(DISALLOW_DOCTYPE, true);
                factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
                factory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
                factory.setFeature(LOAD_EXTERNAL_DTD, false);
                factory.setXIncludeAware(false);
                factory.setExpandEntityReferences(false);
                return factory.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new ProcessorConfigurationException("failed to create document builder", e);
            }
        });
    }

    public DocumentBuilder provide() {
        return localDocumentBuilder.get();
    }
}
