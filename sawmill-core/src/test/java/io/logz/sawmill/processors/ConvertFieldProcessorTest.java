package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.FieldType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class ConvertFieldProcessorTest {
    @Test
    public void testFactory() {
        Map<String,Object> config = new HashMap<>();
        config.put("path", "fieldName");
        config.put("type", "long");

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        assertThat(convertFieldProcessor.getFieldType()).isEqualTo(FieldType.LONG);
    }

    @Test
    public void testConvertToBoolean() {
        testConversion("bool", FieldType.BOOLEAN, "yes", true);
    }

    @Test
    public void testConvertToDouble() {
        testConversion("double", FieldType.DOUBLE, "1.55", 1.55d);
    }

    @Test
    public void testConvertToDoubleStringToZero() {
        testConversion("double", FieldType.DOUBLE, "-", 0D);
    }

    @Test
    public void testConvertToLong() {
        testConversion("long", FieldType.LONG, "12345", 12345L);
    }

    @Test
    public void testConvertToLongStringToZero() {
        testConversion("long", FieldType.LONG, "-", 0L);
    }

    @Test
    public void testConvertToString() {
        testConversion("string", FieldType.STRING, 12345, "12345");
    }

    private <T> void testConversion(String path, FieldType type, Object value, T result) {
        Map<String,Object> config = createConfig("path", path, "type", type.toString());
        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        Doc doc = createDoc(path, value);

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((T) doc.getField(path)).isEqualTo(result);
    }
}
