package io.logz.sawmill.utilities;

import io.logz.sawmill.exceptions.ProcessorConfigurationException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class DocumentBuilderProvider {
    private final ThreadLocal<DocumentBuilder> localDocumentBuilder;

    public DocumentBuilderProvider() {
        localDocumentBuilder = ThreadLocal.withInitial(() -> {
            try {
                return DocumentBuilderFactory.newInstance().newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new ProcessorConfigurationException("failed to create document builder", e);
            }
        });
    }

    public DocumentBuilder provide() {
        return localDocumentBuilder.get();
    }
}
