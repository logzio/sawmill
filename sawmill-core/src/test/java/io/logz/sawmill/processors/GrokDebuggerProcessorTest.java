package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.processors.GrokProcessorTest.APACHE_LOG_SAMPLE;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class GrokDebuggerProcessorTest {

    public static GrokDebuggerProcessor.Factory factory = new GrokDebuggerProcessor.Factory();

    @Test
    public void testValueOffsets() {
        String messageField = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}+%{GREEDYDATA:extra_fields}");

        Doc doc = createDoc(messageField, APACHE_LOG_SAMPLE);

        GrokDebuggerProcessor grokDebuggerProcessor = factory.create(ImmutableMap.of(
                "field", messageField,
                "patterns", patterns
        ));

        ProcessResult processResult = grokDebuggerProcessor.process(doc);
        assertThat(processResult.isSucceeded()).isTrue();

        for (String field : doc.getSource().keySet()) {
            if (field.equals(messageField)) continue;

            Map<String, Object> valueMap = doc.getField(field);
            int start = (int) valueMap.get("start");
            int end = (int) valueMap.get("end");
            assertThat(APACHE_LOG_SAMPLE.substring(start, end)).isEqualTo(valueMap.get("value").toString());
        }
    }
}
