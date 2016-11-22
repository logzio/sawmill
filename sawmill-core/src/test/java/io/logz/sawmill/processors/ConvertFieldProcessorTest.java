package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class ConvertFieldProcessorTest {
    @Test
    public void testFactory() {
        String config = "{ \"path\": \"fieldName\", \"type\": \"long\" }";

        ConvertFieldProcessor convertFieldProcessor = (ConvertFieldProcessor) new ConvertFieldProcessor.Factory().create(config, null);

        assertThat(convertFieldProcessor.getType()).isEqualTo(ConvertFieldProcessor.FieldType.LONG);
    }

    @Test
    public void testConvertToBoolean() {
        String path = "bool";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.BOOLEAN;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type, Collections.EMPTY_LIST, true);

        Doc doc = createDoc(path, "yes");

        convertFieldProcessor.process(doc);

        assertThat((Boolean) doc.getField(path)).isTrue();
    }

    @Test
    public void testConvertToDouble() {
        String path = "double";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.DOUBLE;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type, Collections.EMPTY_LIST, true);

        Doc doc = createDoc(path, "1.55");

        convertFieldProcessor.process(doc);

        assertThat((Double) doc.getField(path)).isEqualTo(1.55d);
    }

    @Test
    public void testConvertToLong() {
        String path = "long";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.LONG;

        ConvertFieldProcessor convertFieldProcessor = new ConvertFieldProcessor(path, type, Collections.EMPTY_LIST, true);

        Doc doc = createDoc(path, "12345");

        convertFieldProcessor.process(doc);

        assertThat((Long) doc.getField(path)).isEqualTo(12345l);
    }
}
