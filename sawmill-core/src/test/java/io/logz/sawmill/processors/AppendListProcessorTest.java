package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class AppendListProcessorTest {

    private static final String FIELD_NAME = "test-field";
    private static final String EXISTING_VALUE = "prev-value";
    private static final String APPENDED_VALUE = "new-value";
    private static final String ANOTHER_VALUE = "another-test";

    @Test
    public void testAppendSingleValueWhenFieldIsNotList() {
        AppendListProcessor appendListProcessor = new AppendListProcessor(FIELD_NAME, Collections.singletonList(APPENDED_VALUE));
        Doc doc = createDoc(FIELD_NAME, EXISTING_VALUE);

        // Tests no exception thrown when there is a field with different type
        assertThat(appendListProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((List) doc.getField(FIELD_NAME)).isEqualTo(Arrays.asList(EXISTING_VALUE, APPENDED_VALUE));
    }

    @Test
    public void testAppendValuesWhileFieldMissing() {
        List<String> values = Arrays.asList(EXISTING_VALUE, APPENDED_VALUE, ANOTHER_VALUE);
        AppendListProcessor appendListProcessor = new AppendListProcessor(FIELD_NAME, values);

        Doc doc = createDoc("field", "value");
        assertThat(appendListProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String) doc.getField("field")).isEqualTo("value");
        assertThat((List) doc.getField(FIELD_NAME)).isEqualTo(values);
    }

    @Test
    public void testAppendValuesWhileFieldExist() {
        List<String> existingList = new ArrayList<>();
        existingList.add(EXISTING_VALUE);

        List<String> values = Arrays.asList(APPENDED_VALUE, ANOTHER_VALUE);
        AppendListProcessor appendListProcessor = new AppendListProcessor(FIELD_NAME, values);
        Doc doc = createDoc(FIELD_NAME, existingList);

        assertThat(appendListProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((List) doc.getField(FIELD_NAME)).isEqualTo(Arrays.asList(EXISTING_VALUE, APPENDED_VALUE, ANOTHER_VALUE));
    }

}