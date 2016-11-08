package io.logz.sawmill.processors;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConvertFieldProcessorTest {
    @Test
    public void testFactory() {
        String config = "{ \"path\": \"fieldName\", \"type\": \"integer\" }";

        ConvertFieldProcessor convertFieldProcessor = (ConvertFieldProcessor) new ConvertFieldProcessor.Factory().create(config);

        assertThat(convertFieldProcessor.getType()).isEqualTo(ConvertFieldProcessor.FieldType.INTEGER);
    }
}
