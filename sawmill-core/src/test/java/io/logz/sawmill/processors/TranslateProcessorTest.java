package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TranslateProcessorTest {
    @Test
    public void testValidTranslation() {
        String field = "field1";
        String targetField = "target";
        Map<String, String> dictionary = ImmutableMap.of("key1", "value1",
                "key2", "value2");
        String fallback = "fallback";

        Map<String, Object> config = createConfig("field", field,
                "targetField", targetField,
                "dictionary", dictionary,
                "fallback", fallback);

        Doc doc = createDoc("field1", "key1");

        TranslateProcessor translateProcessor = createProcessor(TranslateProcessor.class, config);

        ProcessResult processResult = translateProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo("value1");
    }

    @Test
    public void testMissingValueTranslation() {
        String field = "field1";
        String targetField = "target";
        Map<String, String> dictionary = ImmutableMap.of("key1", "value1",
                "key2", "value2");

        Map<String, Object> config = createConfig("field", field,
                "targetField", targetField,
                "dictionary", dictionary);

        Doc doc = createDoc("field1", "key3");

        TranslateProcessor translateProcessor = createProcessor(TranslateProcessor.class, config);

        ProcessResult processResult = translateProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat(doc.hasField(targetField)).isFalse();
    }

    @Test
    public void testMissingValueTranslationWithFallback() {
        String field = "field1";
        String targetField = "target";
        Map<String, String> dictionary = ImmutableMap.of("key1", "value1",
                "key2", "value2");
        String fallback = "fallback";

        Map<String, Object> config = createConfig("field", field,
                "targetField", targetField,
                "dictionary", dictionary,
                "fallback", fallback);

        Doc doc = createDoc("field1", "key3");

        TranslateProcessor translateProcessor = createProcessor(TranslateProcessor.class, config);

        ProcessResult processResult = translateProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo("fallback");
    }

    @Test
    public void testMissingValueTranslationWithFallbackTemplate() {
        String field = "field1";
        String targetField = "target";
        Map<String, String> dictionary = ImmutableMap.of("key1", "value1",
                "key2", "value2");
        String fallback = "{{field2}}";

        Map<String, Object> config = createConfig("field", field,
                "targetField", targetField,
                "dictionary", dictionary,
                "fallback", fallback);

        Doc doc = createDoc("field1", "key3",
                "field2", "value2");

        TranslateProcessor translateProcessor = createProcessor(TranslateProcessor.class, config);

        ProcessResult processResult = translateProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo("value2");
    }

    @Test
    public void testFieldMissingOrNotString() {
        String field = "field1";
        String targetField = "target";
        Map<String, String> dictionary = ImmutableMap.of("key1", "value1",
                "key2", "value2");
        String fallback = "fallback";

        Map<String, Object> config = createConfig("field", field,
                "targetField", targetField,
                "dictionary", dictionary,
                "fallback", fallback);

        Doc doc = createDoc("field1", 1);

        TranslateProcessor translateProcessor = createProcessor(TranslateProcessor.class, config);

        ProcessResult processResult = translateProcessor.process(doc);
        assertThat(processResult.isSucceeded()).isFalse();

        doc = createDoc("anotherField", "value");
        processResult = translateProcessor.process(doc);
        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testBadConfig() {
        assertThatThrownBy(() -> createProcessor(TranslateProcessor.class, "targetField", "target")).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(TranslateProcessor.class, "field", "field")).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(TranslateProcessor.class, "dictionary", ImmutableMap.of("key", "value"))).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(TranslateProcessor.class, "field", "field", "dictionary", "notMap")).isInstanceOf(RuntimeException.class);
    }
}
