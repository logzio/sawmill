package io.logz.sawmill.utilities;

import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DocumentBuilderProviderTest {

    private static final String XML_WITH_FILE_DOCTYPE = "/test_xml_file_injection.xml";
    private static final String XML_WITH_WEB_DOCTYPE = "/test_xml_web_injection.xml";

    @Test
    public void testDocumentBuilderProviderReturnsNonNullEntity() {
        DocumentBuilderProvider documentBuilderProvider = new DocumentBuilderProvider();
        DocumentBuilder documentBuilder = documentBuilderProvider.provide();
        assertThat(documentBuilder).isNotNull();
    }

    @Test
    public void testParseXmlWithBlockedFileDoctype() {
        assertXmlWithForbiddenDocTypeThrowsException(XML_WITH_FILE_DOCTYPE);
    }

    @Test
    public void testParseXmlWithBlockedWebDoctype() {
        assertXmlWithForbiddenDocTypeThrowsException(XML_WITH_WEB_DOCTYPE);
    }

    private void assertXmlWithForbiddenDocTypeThrowsException(String xml) {
        InputStream xmlFile = DocumentBuilderProviderTest.class.getResourceAsStream(xml);
        assertThatThrownBy(() -> new DocumentBuilderProvider().provide().parse(xmlFile))
                .hasMessageStartingWith("DOCTYPE is disallowed");
    }
}