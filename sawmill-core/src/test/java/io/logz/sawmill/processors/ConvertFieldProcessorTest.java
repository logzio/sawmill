package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class ConvertFieldProcessorTest {
    @Test
    public void testFactory() {
        String config = "{ \"path\": \"fieldName\", \"type\": \"long\" }";

        ConvertFieldProcessor convertFieldProcessor = (ConvertFieldProcessor) new ConvertFieldProcessor.Factory().create(config);

        assertThat(convertFieldProcessor.getFieldType()).isEqualTo(ConvertFieldProcessor.FieldType.LONG);
    }

    @Test
    public void testConvertToBoolean() {
        String path = "bool";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.BOOLEAN;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type);

        Doc doc = createDoc(path, "yes");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Boolean) doc.getField(path)).isTrue();
    }

    @Test
    public void testConvertToDouble() {
        String path = "double";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.DOUBLE;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type);

        Doc doc = createDoc(path, "1.55");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Double) doc.getField(path)).isEqualTo(1.55d);
    }

    @Test
    public void testConvertToLong() {
        String path = "long";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.LONG;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type);

        Doc doc = createDoc(path, "12345");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Long) doc.getField(path)).isEqualTo(12345l);
    }
}
