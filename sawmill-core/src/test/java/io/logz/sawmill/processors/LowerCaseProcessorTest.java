package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class LowerCaseProcessorTest {
    @Test
    public void testStringField() {
        String field = "fieldName";
        String value = "CAPITAL LETTERS";

        Doc doc = createDoc(field, value);

        LowerCaseProcessor lowerCaseProcessor = createProcessor(LowerCaseProcessor.class, createConfig("field", field));

        ProcessResult processResult = lowerCaseProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field)).isEqualTo(value.toLowerCase());
    }

    @Test
    public void testIntField() {
        String field = "fieldName";
        int value = 5;

        Doc doc = createDoc(field, value);

        LowerCaseProcessor lowerCaseProcessor = createProcessor(LowerCaseProcessor.class, createConfig("field", field));

        ProcessResult processResult = lowerCaseProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat((int) doc.getField(field)).isEqualTo(value);
    }

    @Test
    public void testNonExistingField() {
        String field = "fieldName";
        int value = 5;

        Doc doc = createDoc("anotherField", value);

        LowerCaseProcessor lowerCaseProcessor = createProcessor(LowerCaseProcessor.class, createConfig("field", field));

        ProcessResult processResult = lowerCaseProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat(doc.hasField(field)).isFalse();
    }
}
