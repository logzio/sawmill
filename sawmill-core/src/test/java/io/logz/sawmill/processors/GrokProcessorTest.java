package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorParseException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrokProcessorTest {
    public static GrokProcessor.Factory factory;

    @BeforeClass
    public static void init() {
        factory = new GrokProcessor.Factory();
    }

    @Test
    public void testSeveralExpressionsWithoutOverwrite() {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}", "%{SYSLOGBASE}");
        String log = "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"";
        String log2 = "Mar 12 12:27:00 server3 named[32172]: lame server resolving 'jakarta5.wasantara.net.id' (in 'wasantara.net.id'?): 202.159.65.171#53";

        Doc doc = createDoc(field, log, "verb", "POST");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("timestamp")).isEqualTo("06/Mar/2013:01:36:30 +0900");
        assertThat((List<String>)doc.getField("verb")).isEqualTo(Arrays.asList("POST", "GET"));
        assertThat((String)doc.getField("response")).isEqualTo("200");
        assertThat((String)doc.getField("HOSTNAME")).isEqualTo("112.169.19.192");
        assertThat((String)doc.getField("agent")).isEqualTo("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22");

        Doc doc2 = createDoc(field, log2);

        ProcessResult processResult2 = grokProcessor.process(doc2);
        assertThat(processResult2.isSucceeded()).isTrue();
        assertThat((String)doc2.getField("timestamp")).isEqualTo("Mar 12 12:27:00");
        assertThat((String)doc2.getField("logsource")).isEqualTo("server3");
        assertThat((String)doc2.getField("pid")).isEqualTo("32172");
        assertThat((String)doc2.getField("program")).isEqualTo("named");
    }

    @Test
    public void testOverwrite() {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}");
        String log = "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"";

        Doc doc = createDoc(field, log, "verb", "POST");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        config.put("overwrite", Arrays.asList("verb"));
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("timestamp")).isEqualTo("06/Mar/2013:01:36:30 +0900");
        assertThat((String)doc.getField("verb")).isEqualTo("GET");
        assertThat((String)doc.getField("response")).isEqualTo("200");
        assertThat((String)doc.getField("HOSTNAME")).isEqualTo("112.169.19.192");
        assertThat((String)doc.getField("agent")).isEqualTo("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22");
    }

    @Test
    public void testIgnoreMissing() {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}");

        Doc doc = createDoc("differentField", "value");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
    }

    @Test
    public void testWithoutIgnoreMissing() {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}");

        Doc doc = createDoc("differentField", "value");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        config.put("ignoreMissing", false);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testConfigWithoutPatterns() {
        Map<String,Object> config = new HashMap<>();
        config.put("field", "someField");

        assertThatThrownBy(() -> factory.create(config)).isInstanceOf(ProcessorParseException.class);
    }

    @Test
    public void testWithNonExistsPattern() {
        Map<String,Object> config = new HashMap<>();
        config.put("field", "someField");
        config.put("patterns", Arrays.asList("%{NONEXISTSPATTERN}"));

        assertThatThrownBy(() -> factory.create(config)).isInstanceOf(RuntimeException.class);
    }
}
