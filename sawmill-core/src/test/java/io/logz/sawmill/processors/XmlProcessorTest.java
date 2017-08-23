package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class XmlProcessorTest {

    public static final String VALID_XML =
            "<country>" +
                "<id>1</id>" +
                "<name>Israel</name>" +
                "<cities>" +
                    "<city>" +
                        "<name>Jerusalem</name>" +
                    "</city>" +
                    "<city>" +
                        "<name>Tel Aviv</name>" +
                    "</city>" +
                "</cities>" +
                "<lat>31.0461</lat>" +
                "<long>34.8516</long>" +
                "<continent>Asia</continent>" +
                "<currency>New Shekel</currency>" +
                "<languages>" +
                    "<language type=\"official\">Hebrew</language>" +
                    "<language type=\"official\">Arabic</language>" +
                    "<language>English</language>" +
                "</languages>" +
            "</country>";

    public static final String INVALID_XML = "<invalid>/invalid";

    @Test
    public void testValidXml() {
        String field = "xml";

        Doc doc = createDoc(field, VALID_XML);

        XmlProcessor xmlProcessor = createProcessor(XmlProcessor.class, "field", field);

        ProcessResult processResult = xmlProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("country.id")).isEqualTo("1");
        assertThat((String) doc.getField("country.name")).isEqualTo("Israel");
        assertThat((List) doc.getField("country.cities.city"))
                .isEqualTo(Arrays.asList(ImmutableMap.of("name", "Jerusalem"),
                        ImmutableMap.of("name", "Tel Aviv")));
        assertThat((String) doc.getField("country.lat")).isEqualTo("31.0461");
        assertThat((String) doc.getField("country.long")).isEqualTo("34.8516");
        assertThat((String) doc.getField("country.continent")).isEqualTo("Asia");
        assertThat((String) doc.getField("country.currency")).isEqualTo("New Shekel");
        assertThat((List) doc.getField("country.languages.language")).isEqualTo(Arrays.asList("Hebrew", "Arabic", "English"));
    }

    @Test
    public void testXPath() {
        String field = "xml";

        Doc doc = createDoc(field, VALID_XML);

        Map<String, Object> config = createConfig("field", field,
                "storeXml", false,
                "xpath", ImmutableMap.of("/country/languages/language[@type='official']/text()", "lang",
                        "/country/languages/language[2]/text()", "lang",
                        "/country/cities/city[2]/name/text()", "bestCity",
                        "/country/otherField", "nonExistsField"));

        XmlProcessor xmlProcessor = createProcessor(XmlProcessor.class, config);

        ProcessResult processResult = xmlProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((List) doc.getField("lang")).isEqualTo(Arrays.asList("Hebrew", "Arabic"));
        assertThat((String) doc.getField("bestCity")).isEqualTo("Tel Aviv");
        assertThat(doc.hasField("nonExistsField")).isFalse();
    }

    @Test
    public void testInvalidXml() {
        String field = "xml";

        Doc doc = createDoc(field, INVALID_XML);

        XmlProcessor xmlProcessor = createProcessor(XmlProcessor.class, "field", field);

        ProcessResult processResult = xmlProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testInvalidXpath() {
        Map<String, Object> config = createConfig("field", "someField",
                "xpath", ImmutableMap.of("/co'untry/la()ngu", "noooo"));

        assertThatThrownBy(() -> createProcessor(XmlProcessor.class, config)).isInstanceOf(ProcessorConfigurationException.class);
    }
}
