package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class GrokProcessorTest {
    @Test
    public void testGrok() {
        String field = "message";
        String log = "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"";
        String pattern = "%{COMBINEDAPACHELOG}";

        Doc doc = createDoc(field, log);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("pattern", pattern);
        GrokProcessor grokProcessor = new GrokProcessor.Factory().create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.getSource().size()).isEqualTo(26);
        assertThat((String)doc.getField("timestamp")).isEqualTo("06/Mar/2013:01:36:30 +0900");
        assertThat((String)doc.getField("verb")).isEqualTo("GET");
        assertThat((String)doc.getField("response")).isEqualTo("200");
        assertThat((String)doc.getField("HOSTNAME")).isEqualTo("112.169.19.192");
        assertThat((String)doc.getField("agent")).isEqualTo("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22");
    }
}
