package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class Base64DecodeProcessorTest {

    @Test
    public void testNullOrEmptyConfigurationFieldsShouldFailCreator() {
        Stream.of(
                createConfig(null, "target"),
                createConfig("", "target"),
                createConfig("source", null),
                createConfig("source", ""))
        .forEach((config) -> assertThatThrownBy(() -> createProcessor(Base64DecodeProcessor.class, config))
                .isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("sourceField, targetField can not be null or empty"));
    }

    @Test
    public void testDecode() {
        Map<String, Object> config = new HashMap<>();
        config.put("sourceField", "message");
        config.put("targetField", "message_decoded");

        Map<String, Object> map = new HashMap<>();
        String encodedMessage = Base64.getEncoder().encodeToString("testEmptyFieldsShouldFail".getBytes());
        map.put("message", encodedMessage);
        Doc doc = new Doc(map);

        ProcessResult result;
        Processor processor = createProcessor(Base64DecodeProcessor.class, config);
        try {
            result = processor.process(doc);
        } catch (InterruptedException e) { throw new RuntimeException(e); }

        assertThat(result != null && result.isSucceeded()).isTrue();
        assertThat(doc.getField(config.get("sourceField").toString()).toString()).isEqualTo(encodedMessage);
        assertThat(doc.getField(config.get("targetField").toString()).toString())
                .isEqualTo("testEmptyFieldsShouldFail");
    }

    @Test
    public void testNonStringFieldShouldFail() {
        Map<String, Object> config = new HashMap<>();
        config.put("sourceField", "numberField");
        config.put("targetField", "noop");

        Map<String, Object> map = new HashMap<>();
        map.put("message", "testSingleNonStringFieldShouldFail");
        map.put("numberField", 123);
        Doc doc = new Doc(map);

        Processor processor = createProcessor(Base64DecodeProcessor.class, config);

        ProcessResult result;

        try {
            result = processor.process(doc);
        } catch (InterruptedException e) { throw new RuntimeException(e); }
        assertThat(result != null && !result.isSucceeded()).isTrue();
        assertThat(result.getError().isPresent()).isTrue();
        assertThat(result.getError().get().getMessage())
                .isEqualTo("field is missing from doc");
        assertThat(doc.hasField(config.get("targetField").toString())).isFalse();
        assertThat(doc.getField("message").toString()).isEqualTo(map.get("message").toString());
    }

    @Test
    public void testMissingFieldShouldFail() {
        Map<String, Object> config = new HashMap<>();
        config.put("sourceField", "foo");
        config.put("targetField", "foo_decoded");

        Map<String, Object> map = new HashMap<>();
        map.put("message", "testAllFieldsMissingShouldFail");

        Doc doc = new Doc(map);

        ProcessResult result;
        Processor processor = createProcessor(Base64DecodeProcessor.class, config);
        try {
            result = processor.process(doc);
        } catch (InterruptedException e) { throw new RuntimeException(e); }

        assertThat(result != null && !result.isSucceeded()).isTrue();
        assertThat(result.getError().get().getMessage())
                .isEqualTo("field is missing from doc");
        assertThat(doc.hasField(config.get("targetField").toString())).isFalse();
        assertThat(doc.getField("message").toString()).isEqualTo(map.get("message").toString());
    }

    private Map<String, Object> createConfig(String sourceField, String targetField) {
        Map<String, Object> config = new HashMap<>();
        config.put("sourceField", sourceField);
        config.put("targetField", targetField);
        return config;
    }
}
