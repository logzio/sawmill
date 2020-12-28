package io.logz.sawmill.processors;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

public class XmlProcessorTest {

    public static String VALID_XML;

    static {
        try {
            VALID_XML = IOUtils.toString(XmlProcessorTest.class.getResourceAsStream("/xml_valid.xml"), Charsets.UTF_8);
        } catch (IOException e) {
            fail("couldn't load valid xml");
        }
    }

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
        assertThat((Object) doc.getField("country.TestEmptyField")).isEqualTo(IllegalStateException.class);
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
