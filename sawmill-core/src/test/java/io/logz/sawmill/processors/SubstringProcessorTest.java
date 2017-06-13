package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class SubstringProcessorTest {
    @Test
    public void testStringField() {
        String field = "message";
        int begin = 5;
        int end = 20;
        String value = "this is longer than range string, substring me";

        SubstringProcessor substringProcessor = createProcessor(SubstringProcessor.class, "field", field, "begin", begin, "end", end);

        Doc doc = createDoc(field, value);
        ProcessResult processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field)).isEqualTo(value.substring(begin, end));

        value = "this is short";
        doc = createDoc(field, value);
        processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field)).isEqualTo(value.substring(begin));

        value = "fail";
        doc = createDoc(field, value);
        processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();

        value = "this is a substring without end";
        doc = createDoc(field, value);
        
        substringProcessor = createProcessor(SubstringProcessor.class, "field", field, "begin", begin);
        processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field)).isEqualTo(value.substring(begin));
    }

    @Test
    public void testDifferentFields() {
        String field = "message";
        int begin = 5;
        int end = 20;
        Object value = 1;

        SubstringProcessor substringProcessor = createProcessor(SubstringProcessor.class, "field", field, "begin", begin, "end", end);

        Doc doc = createDoc(field, value);
        ProcessResult processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();

        value = true;
        doc = createDoc(field, value);
        processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();

        value = Arrays.asList("fail");
        doc = createDoc(field, value);
        processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();

        value = Collections.singletonMap("a", "map");
        doc = createDoc(field, value);
        processResult = substringProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }
}
