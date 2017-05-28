package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.FieldType;
import io.logz.sawmill.ProcessResult;
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
        config.put("paths", Arrays.asList("fieldName", "fieldName2", "fieldName3"));
        config.put("type", "long");

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        assertThat(convertFieldProcessor.getFieldType()).isEqualTo(FieldType.LONG);
    }

    @Test
    public void testFactoryFailsWhenBothPathAndMultiPathsExist() {
        Map<String,Object> config = new HashMap<>();
        config.put("paths", Arrays.asList("fieldName", "fieldName2", "fieldName3"));
        config.put("path", "fieldName4");
        config.put("type", "long");

        assertThatThrownBy(() -> createProcessor(ConvertFieldProcessor.class, config)).isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("both field path and field paths are defined when only 1 allowed");
    }

    @Test
    public void testConvertSeveralFieldsToLong() {
        Map<String,Object> config = new HashMap<>();
        config.put("paths", Arrays.asList("field1", "field2", "field3", "nonExistsField"));
        config.put("type", "long");

        Doc doc = createDoc("field1", "10",
                "field2", "should be 0",
                "field3", 5);

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        ProcessResult processResult = convertFieldProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        String errorMessage = processResult.getError().get().getMessage();
        assertThat(errorMessage.contains("nonExistsField")).isTrue();
        assertThat(doc.hasField("field1", Long.class)).isTrue();
        assertThat(doc.hasField("field2", Long.class)).isTrue();
        assertThat(doc.hasField("field3", Long.class)).isTrue();
    }

    @Test
    public void testConvertSeveralFieldsToDouble() {
        Map<String,Object> config = new HashMap<>();
        config.put("paths", Arrays.asList("field1", "field2", "field3", "nonExistsField"));
        config.put("type", "double");

        Doc doc = createDoc("field1", "10",
                "field2", "should be 0",
                "field3", 5);

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        ProcessResult processResult = convertFieldProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        String errorMessage = processResult.getError().get().getMessage();
        assertThat(errorMessage.contains("nonExistsField")).isTrue();
        assertThat(doc.hasField("field1", Double.class)).isTrue();
        assertThat(doc.hasField("field2", Long.class)).isTrue(); // Zero is long
        assertThat(doc.hasField("field3", Double.class)).isTrue();
    }

    @Test
    public void testConvertSeveralFieldsToString() {
        Map<String,Object> config = new HashMap<>();
        config.put("paths", Arrays.asList("field1", "field2", "field3", "nonExistsField"));
        config.put("type", "string");

        Doc doc = createDoc("field1", 50l,
                "field2", "should be 0",
                "field3", 5);

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        ProcessResult processResult = convertFieldProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        String errorMessage = processResult.getError().get().getMessage();
        assertThat(errorMessage.contains("nonExistsField")).isTrue();
        assertThat(doc.hasField("field1", String.class)).isTrue();
        assertThat(doc.hasField("field2", String.class)).isTrue();
        assertThat(doc.hasField("field3", String.class)).isTrue();
    }

    @Test
    public void testConvertSeveralFieldsToBoolean() {
        Map<String,Object> config = new HashMap<>();
        config.put("paths", Arrays.asList("field1", "field2", "field3", "nonExistsField"));
        config.put("type", "boolean");

        Doc doc = createDoc("field1", "yes",
                "field2", "cannot convert",
                "field3", 5);

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        ProcessResult processResult = convertFieldProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        String errorMessage = processResult.getError().get().getMessage();
        assertThat(errorMessage.contains("nonExistsField")).isTrue();
        assertThat(errorMessage.contains("field2")).isTrue();
        assertThat(errorMessage.contains("field3")).isTrue();
        assertThat(doc.hasField("field1", Boolean.class)).isTrue();
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
        testConversion("-", 0l);
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
