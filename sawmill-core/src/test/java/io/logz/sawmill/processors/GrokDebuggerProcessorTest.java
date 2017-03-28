package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.processors.GrokProcessorTest.APACHE_LOG_SAMPLE;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createFactory;
import static org.assertj.core.api.Assertions.assertThat;

public class GrokDebuggerProcessorTest {

    public static GrokDebuggerProcessor.Factory factory = createFactory(GrokDebuggerProcessor.class);

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

    @Test
    public void testListValueOffset() {
        String field = "message";
        List<String> patterns = Arrays.asList("%{WORD:http-verb} %{WORD:http-verb}");

        String text = "GET POST";
        Doc doc = createDoc(field, text);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokDebuggerProcessor grokDebuggerProcessor = factory.create(ImmutableMap.of(
                "field", field,
                "patterns", patterns
        ));

        ProcessResult processResult = grokDebuggerProcessor.process(doc);
        assertThat(processResult.isSucceeded()).isTrue();

        List<Map<String, Object>> valueList = doc.getField("http-verb");
        assertThat(valueList).hasSize(2);

        Map<String, Object> verb1 = valueList.get(0);
        int start1 = (int) verb1.get("start");
        int end1 = (int) verb1.get("end");
        assertThat(text.substring(start1, end1)).isEqualTo(verb1.get("value").toString());

        Map<String, Object> verb2 = valueList.get(1);
        int start2 = (int) verb2.get("start");
        int end2 = (int) verb2.get("end");
        assertThat(text.substring(start2, end2)).isEqualTo(verb2.get("value").toString());
    }
}
