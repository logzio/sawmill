package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static java.util.Collections.EMPTY_LIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UpperCaseProcessorTest {
    @Test
    public void testBadConfig() {
        assertThatThrownBy(() -> createProcessor(UpperCaseProcessor.class, "fields", null)).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(UpperCaseProcessor.class, "fields", EMPTY_LIST)).isInstanceOf(ProcessorConfigurationException.class);
    }

    @Test
    public void testListOfStringFields() {
        List<String> fields = Arrays.asList("field1", "field2", "field3");

        Doc doc = createDoc("field1", "lower case",
                "field2", "camelCase",
                "field3", "UPPER CASE");

        UpperCaseProcessor upperCaseProcessor = createProcessor(UpperCaseProcessor.class, "fields", fields);

        ProcessResult processResult = upperCaseProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("field1")).isEqualTo("LOWER CASE");
        assertThat((String) doc.getField("field2")).isEqualTo("CAMELCASE");
        assertThat((String) doc.getField("field3")).isEqualTo("UPPER CASE");
    }

    @Test
    public void testListWithNonStringFields() {
        List<String> fields = Arrays.asList("field1", "field2", "field3", "nonExistsField");

        Doc doc = createDoc("field1", "lower case",
                "field2", 1,
                "field3", true);

        UpperCaseProcessor upperCaseProcessor = createProcessor(UpperCaseProcessor.class, "fields", fields);

        ProcessResult processResult = upperCaseProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        String errorMessage = processResult.getError().get().getMessage();
        assertThat((String) doc.getField("field1")).isEqualTo("LOWER CASE");
        assertThat(errorMessage.contains("field2")).isTrue();
        assertThat(errorMessage.contains("field3")).isTrue();
        assertThat(errorMessage.contains("nonExistsField")).isTrue();
    }
}
