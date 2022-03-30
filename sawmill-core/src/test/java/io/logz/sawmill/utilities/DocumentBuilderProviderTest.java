package io.logz.sawmill.utilities;

import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DocumentBuilderProviderTest {

    private static final String XML_FILE = "/test_xml_injection.xml";

    @Test
    public void testDocumentBuilderProviderReturnsNonNullEntity() {
        DocumentBuilderProvider documentBuilderProvider = new DocumentBuilderProvider();
        DocumentBuilder documentBuilder = documentBuilderProvider.provide();
        assertThat(documentBuilder).isNotNull();
    }

    @Test
    public void testParseXml() {
        InputStream xmlFile = DocumentBuilderProviderTest.class.getResourceAsStream(XML_FILE);
        assertThatThrownBy(() -> new DocumentBuilderProvider().provide().parse(xmlFile))
                .hasMessageStartingWith("DOCTYPE is disallowed");
    }
}