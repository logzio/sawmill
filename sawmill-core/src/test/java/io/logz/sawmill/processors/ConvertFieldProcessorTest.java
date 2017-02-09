package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class ConvertFieldProcessorTest {
    @Test
    public void testConvertToBoolean() {
        String path = "bool";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.BOOLEAN;

        Map<String,Object> config = createConfig("path", path, "type", type.toString());

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        Doc doc = createDoc(path, "yes");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Boolean) doc.getField(path)).isTrue();
    }

    @Test
    public void testConvertToDouble() {
        String path = "double";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.DOUBLE;

        Map<String,Object> config = createConfig("path", path, "type", type.toString());

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        Doc doc = createDoc(path, "1.55");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Double) doc.getField(path)).isEqualTo(1.55d);
    }

    @Test
    public void testConvertToLong() {
        String path = "long";
        ConvertFieldProcessor.FieldType type = ConvertFieldProcessor.FieldType.LONG;

        Map<String,Object> config = createConfig("path", path, "type", type.toString());

        ConvertFieldProcessor convertFieldProcessor = createProcessor(ConvertFieldProcessor.class, config);

        Doc doc = createDoc(path, "12345");

        assertThat(convertFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((Long) doc.getField(path)).isEqualTo(12345l);
    }
}
