package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class StripProcessorTest {
    @Test
    public void testStringFields() {
        List<String> fields = Arrays.asList("field1", "field2", "field3", "field4");

        Doc doc = createDoc("field1", "   needs to be stripped    ",
                "field2", "all Good",
                "field3", "only After    ",
                "field4", "    only Before");

        StripProcessor stripProcessor = createProcessor(StripProcessor.class, createConfig("fields", fields));

        ProcessResult processResult = stripProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("field1")).isEqualTo("needs to be stripped");
        assertThat((String) doc.getField("field2")).isEqualTo("all Good");
        assertThat((String) doc.getField("field3")).isEqualTo("only After");
        assertThat((String) doc.getField("field4")).isEqualTo("only Before");
    }

    @Test
    public void testWithFailedFields() {
        List<String> fields = Arrays.asList("field1", "field2", "field3", "field4", "nonExistsField");

        Doc doc = createDoc("field1", true,
                "field2", 5,
                "field3", "   needs to be stripped    ",
                "field4", Arrays.asList("this", "is", "not", "good"));

        StripProcessor stripProcessor = createProcessor(StripProcessor.class, createConfig("fields", fields));

        ProcessResult processResult = stripProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat((String) doc.getField("field3")).isEqualTo("needs to be stripped");

        String failureMessage = processResult.getError().get().getMessage();
        assertThat(failureMessage).contains("field1");
        assertThat(failureMessage).contains("field2");
        assertThat(failureMessage).contains("field4");
        assertThat(failureMessage).contains("nonExistsField");
    }
}
