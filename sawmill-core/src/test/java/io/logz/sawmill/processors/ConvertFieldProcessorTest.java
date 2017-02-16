package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.FieldType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class ConvertFieldProcessorTest {
    @Test
    public void testFactory() {
        Map<String,Object> config = new HashMap<>();
        config.put("path", "fieldName");
        config.put("type", "long");

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor.Factory().create(config);

        assertThat(convertFieldProcessor.getFieldType()).isEqualTo(FieldType.LONG);
    }

    @Test
    public void testConvertToBoolean() {
        String path = "bool";
        FieldType type = FieldType.BOOLEAN;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type);

        Doc doc = createDoc(path, "yes");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Boolean) doc.getField(path)).isTrue();
    }

    @Test
    public void testConvertToDouble() {
        String path = "double";
        FieldType type = FieldType.DOUBLE;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type);

        Doc doc = createDoc(path, "1.55");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Double) doc.getField(path)).isEqualTo(1.55d);
    }

    @Test
    public void testConvertToLong() {
        String path = "long";
        FieldType type = FieldType.LONG;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type);

        Doc doc = createDoc(path, "12345");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Long) doc.getField(path)).isEqualTo(12345l);
    }

    @Test
    public void testConvertToString() {
        String path = "string";
        FieldType type = FieldType.STRING;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type);

        Doc doc = createDoc(path, 12345);

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String) doc.getField(path)).isEqualTo("12345");
    }
}
