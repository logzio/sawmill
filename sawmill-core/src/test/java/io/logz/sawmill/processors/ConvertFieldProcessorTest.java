package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.FieldType;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ConvertFieldProcessorTest {

    @Test
    public void testFactoryOfSinglePath() {
        Map<String,Object> config = new HashMap<>();
        config.put("path", "fieldName");
        config.put("type", "long");

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        assertThat(convertFieldProcessor.getFieldType()).isEqualTo(FieldType.LONG);
    }

    @Test
    public void testFactoryOfMultiPaths() {
        Map<String,Object> config = new HashMap<>();
        config.put("paths", Arrays.asList("fieldName,fieldName2,fieldName3"));
        config.put("type", "long");

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        assertThat(convertFieldProcessor.getFieldType()).isEqualTo(FieldType.LONG);
    }

    @Test
    public void testFactoryFailsWhenBothPathAndMultiPathsExist() {
        Map<String,Object> config = new HashMap<>();
        config.put("paths", Arrays.asList("fieldName,fieldName2,fieldName3"));
        config.put("path", "fieldName4");
        config.put("type", "long");

        assertThatThrownBy(() -> createProcessor(ConvertFieldProcessor.class, config)).isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("both field path and field paths are defined when only 1 allowed");
    }

    @Test
    public void testConvertToBoolean() {
        testConversion("yes", true);
    }

    @Test
    public void testConvertToDouble() {
        testConversion("1.55", 1.55d);
    }

    @Test
    public void testConvertToDoubleDefaultIsZero() {
        testConversion("-", 0D);
    }

    @Test
    public void testConvertToLong() {
        testConversion( "12345", 12345L);
    }

    @Test
    public void testConvertToLongDefaultIsZero() {
        testConversion("-", 0L);
    }

    @Test
    public void testConvertToString() {
        testConversion(12345, "12345");
    }

    private <T> void testConversion( Object value, T result) {
        String resultClassName = result.getClass().getSimpleName().toLowerCase();
        String path = RandomStringUtils.random(5);
        Map<String,Object> config = createConfig("path", path, "type", resultClassName);
        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        Doc doc = createDoc(path, value);

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((T) doc.getField(path)).isEqualTo(result);
    }
}
