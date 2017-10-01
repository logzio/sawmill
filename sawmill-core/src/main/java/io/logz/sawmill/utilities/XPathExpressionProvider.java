package io.logz.sawmill.utilities;

import io.logz.sawmill.exceptions.ProcessorConfigurationException;

import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public class XPathExpressionProvider {
    private final ThreadLocal<XPathExpression> localXPathExpression;

    public XPathExpressionProvider(String expression) {
        localXPathExpression = ThreadLocal.withInitial(() -> {
            try {
                return XPathFactory.newInstance().newXPath().compile(expression);
            } catch (XPathExpressionException e) {
                throw new ProcessorConfigurationException("failed to create xpath expression", e);
            }
        });
    }

    public XPathExpression provide() {
        return localXPathExpression.get();
    }
}
