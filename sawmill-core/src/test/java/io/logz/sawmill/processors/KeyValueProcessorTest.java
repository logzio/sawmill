package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class KeyValueProcessorTest {

    public static final String DEFAULT_SEPARATORS_KEV_VALUE_MESSAGE = "this is key-value with default separators, the key-values start from here " +
            "simple=value " +
            "brackets=[with space] " +
            "roundBrackets=(with two space) " +
            "angleBrackets=<without> " +
            "curlyBrackets={again space} " +
            "complex=(innerKey=innerValue withBrackets=(another innerValue))";

    @Test
    public void testDefault() {
        String field = "message";
        Doc doc = createDoc(field, DEFAULT_SEPARATORS_KEV_VALUE_MESSAGE);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("recursive", true);

        KeyValueProcessor kvProcessor = new KeyValueProcessor.Factory().create(config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();

    }
}
