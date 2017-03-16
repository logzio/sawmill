package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class SplitProcessorTest {
    @Test
    public void testStringFieldNotContainedSeparator() {
        String field = "fieldName";
        String value = "this string is gonna be without any commas";

        Doc doc = createDoc(field, value);

        SplitProcessor splitProcessor = createProcessor(SplitProcessor.class, createConfig("field", field, "separator", ","));

        ProcessResult processResult = splitProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field)).isEqualTo(value);
    }

    @Test
    public void testStringFieldWithCharacterSeparator() {
        String field = "fieldName";
        String value = "lets,split,it,yo";
        String separator = ",";

        Doc doc = createDoc(field, value);

        SplitProcessor splitProcessor = createProcessor(SplitProcessor.class, createConfig("field", field, "separator", separator));

        ProcessResult processResult = splitProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(field, List.class)).isTrue();
        assertThat((List) doc.getField(field)).isEqualTo(Arrays.asList("lets", "split", "it", "yo"));
    }

    @Test
    public void testStringFieldWithRegexSeparator() {
        String field = "fieldName";
        String value = "split11by0000regex5yo";
        String separator = "[0-9]+";

        Doc doc = createDoc(field, value);

        SplitProcessor splitProcessor = createProcessor(SplitProcessor.class, createConfig("field", field, "separator", separator));

        ProcessResult processResult = splitProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(field, List.class)).isTrue();
        assertThat((List) doc.getField(field)).isEqualTo(Arrays.asList("split", "by", "regex", "yo"));
    }

    @Test
    public void testIntField() {
        String field = "fieldName";
        int value = 5;

        Doc doc = createDoc(field, value);

        SplitProcessor splitProcessor = createProcessor(SplitProcessor.class, createConfig("field", field, "separator", ","));

        ProcessResult processResult = splitProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat((int) doc.getField(field)).isEqualTo(value);
    }

    @Test
    public void testNonExistingField() {
        String field = "fieldName";
        int value = 5;

        Doc doc = createDoc("anotherField", value);

        SplitProcessor splitProcessor = createProcessor(SplitProcessor.class, createConfig("field", field, "separator", ","));

        ProcessResult processResult = splitProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat(doc.hasField(field)).isFalse();
    }
}
