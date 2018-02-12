package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AhoCorasickProcessorTest {

    @Test
    public void testInputWords() {
        String field = "message";
        String targetField = "auocorasick";
        List<String> inputWords = Arrays.asList("match1", "match two");

        Doc doc = createDoc(field, "this is a match for match1 and match two");

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "inputWords", inputWords);

        AhoCorasickProcessor processor = createProcessor(AhoCorasickProcessor.class, config);

        assertThat(processor.process(doc).isSucceeded()).isTrue();

        List<String> output = doc.getField(targetField);
        assertThat(output).isNotEmpty();
        assertThat(output.size()).isEqualTo(2);
        assertThat(output).isEqualTo(inputWords);
    }

    @Test
    public void testFail() {
        String field = "message";
        String targetField = "auocorasick";
        List<String> inputWords = Arrays.asList("match one", "match 2");

        Doc doc = createDoc(field, "this is a match for match1 and match two");

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "inputWords", inputWords);

        AhoCorasickProcessor processor = createProcessor(AhoCorasickProcessor.class, config);

        assertFail(targetField, doc, processor);
    }

    @Test
    public void testBadConfig() {
        String field = "message";
        String targetField = "auocorasick";
        List<String> inputWords = Arrays.asList("match one", "match 2");

        Doc doc = createDoc(field, "this is a match for match1 and match two");

        Map<String,Object> config = createConfig("field", "notExistingField",
                "targetField", targetField,
                "inputWords", inputWords);

        AhoCorasickProcessor processor = createProcessor(AhoCorasickProcessor.class, config);

        assertFail(targetField, doc, processor);

        config = createConfig("field", field,
                "targetField", "notExistingTargetField",
                "inputWords", inputWords);

        processor = createProcessor(AhoCorasickProcessor.class, config);

        assertFail(targetField, doc, processor);

        assertThatThrownBy(() -> createProcessor(AhoCorasickProcessor.class,
                createConfig("field", field, "targetField", targetField, "inputWords", new ArrayList<>())))
                .isInstanceOf(ProcessorConfigurationException.class);
    }

    private void assertFail(String targetField, Doc doc, AhoCorasickProcessor processor) {
        assertThat(processor.process(doc).isSucceeded()).isFalse();
        assertThat(doc.hasField(targetField)).isFalse();
    }

}
