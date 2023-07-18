package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class Base64DecodeProcessorTest {

    @Test
    public void testEmptyFieldsShouldFail() {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.emptySet());
        config.put("allowMissingFields", false);

        assertThatThrownBy(() -> createProcessor(Base64DecodeProcessor.class, config))
                .isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("fields can not be empty");
    }

    @Test
    public void testDecode() {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singleton("message"));

        Map<String, Object> map = new HashMap<>();
        map.put("message", Base64.getEncoder().encodeToString("testEmptyFieldsShouldFail".getBytes()));
        Doc doc = new Doc(map);

        ProcessResult result;
        Processor processor = createProcessor(Base64DecodeProcessor.class, config);
        try {
            result = processor.process(doc);
        } catch (InterruptedException e) { throw new RuntimeException(e); }

        assertThat(result != null && result.isSucceeded()).isTrue();
        assertThat(doc.getField("message").toString()).isEqualTo("testEmptyFieldsShouldFail");
    }

    @Test
    public void testSingleNonStringFieldShouldFail() {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singleton("numberField"));
        config.put("allowMissingFields", false);

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
                .isEqualTo("some or all fields are missing from doc");
    }

    @Test
    public void testDecodeMultipleFields() {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Stream.of("foo", "bar", "foobar").collect(Collectors.toSet()));

        Map<String, Object> map = new HashMap<>();
        map.put("message", "testDecodeMultipleFields");
        map.put("foo", Base64.getEncoder().encodeToString("foo".getBytes()));
        map.put("bar", Base64.getEncoder().encodeToString("bar".getBytes()));
        map.put("foobar", Base64.getEncoder().encodeToString("foobar".getBytes()));

        Doc doc = new Doc(map);

        ProcessResult result;
        Processor processor = createProcessor(Base64DecodeProcessor.class, config);
        try {
            result = processor.process(doc);
        } catch (InterruptedException e) { throw new RuntimeException(e); }

        assertThat(result != null && result.isSucceeded()).isTrue();
        assertThat(doc.getField("message").toString()).isEqualTo("testDecodeMultipleFields");
        assertThat(doc.getField("foo").toString()).isEqualTo("foo");
        assertThat(doc.getField("bar").toString()).isEqualTo("bar");
        assertThat(doc.getField("foobar").toString()).isEqualTo("foobar");
    }

    @Test
    public void testAllFieldsMissingShouldFail() {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Stream.of("foo", "bar", "foobar").collect(Collectors.toSet()));

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
                .isEqualTo("some or all fields are missing from doc");
    }

    @Test
    public void testSomeFieldsMissingShouldSucceedIfAllowed() {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Stream.of("foo", "bar", "foobar").collect(Collectors.toSet()));
        config.put("allowMissingFields", true);

        Map<String, Object> map = new HashMap<>();
        map.put("message", "testAllFieldsMissingShouldFail");
        map.put("foo", Base64.getEncoder().encodeToString("foo".getBytes()));

        Doc doc = new Doc(map);

        ProcessResult result;
        Processor processor = createProcessor(Base64DecodeProcessor.class, config);
        try {
            result = processor.process(doc);
        } catch (InterruptedException e) { throw new RuntimeException(e); }

        assertThat(result != null && result.isSucceeded()).isTrue();
        assertThat(doc.getField("foo").toString()).isEqualTo("foo");
    }

    @Test
    public void testSomeFieldsMissingShouldFailIfNotAllowed() {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Stream.of("foo", "bar", "foobar").collect(Collectors.toSet()));
        config.put("allowMissingFields", false);

        Map<String, Object> map = new HashMap<>();
        map.put("message", "testAllFieldsMissingShouldFail");
        map.put("foo", Base64.getEncoder().encodeToString("foo".getBytes()));

        Doc doc = new Doc(map);

        ProcessResult result;
        Processor processor = createProcessor(Base64DecodeProcessor.class, config);
        try {
            result = processor.process(doc);
        } catch (InterruptedException e) { throw new RuntimeException(e); }

        assertThat(result != null && !result.isSucceeded()).isTrue();
        assertThat(result.getError().get().getMessage())
                .isEqualTo("some or all fields are missing from doc");
    }
}
