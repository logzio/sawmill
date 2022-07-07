package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static io.logz.sawmill.utils.FactoryUtils.createProcessorFactory;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrokProcessorTest {
    public static final String SYS_LOG_SAMPLE = "Mar 12 12:27:00 server3 named[32172]: lame server resolving 'jakarta5.wasantara.net.id' (in 'wasantara.net.id'?): 202.159.65.171#53";
    public static final String APACHE_LOG_SAMPLE = "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"";
    public static GrokProcessor.Factory factory;

    @BeforeClass
    public static void init() {
        factory = createProcessorFactory(GrokProcessor.class);
    }

    @Test
    public void testSeveralExpressions() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}", "%{SYSLOGBASE}");

        Doc doc = createDoc(field, APACHE_LOG_SAMPLE);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);
        assertApacheLog(doc, processResult);

        Doc doc2 = createDoc(field, SYS_LOG_SAMPLE);

        ProcessResult processResult2 = grokProcessor.process(doc2);
        assertSysLog(doc2, processResult2);
    }

    private void assertSysLog(Doc doc2, ProcessResult processResult2) {
        assertThat(processResult2.isSucceeded()).isTrue();
        assertThat((String)doc2.getField("timestamp")).isEqualTo("Mar 12 12:27:00");
        assertThat((String)doc2.getField("logsource")).isEqualTo("server3");
        assertThat((String)doc2.getField("pid")).isEqualTo("32172");
        assertThat((String)doc2.getField("program")).isEqualTo("named");
    }

    private void assertApacheLog(Doc doc, ProcessResult processResult) {
        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("timestamp")).isEqualTo("06/Mar/2013:01:36:30 +0900");
        assertThat((String)doc.getField("response")).isEqualTo("200");
        assertThat((String)doc.getField("verb")).isEqualTo("GET");
        assertThat((String)doc.getField("clientip")).isEqualTo("112.169.19.192");
        assertThat((String)doc.getField("agent")).isEqualTo("\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
    }

    @Test
    public void testOverwrite() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}+%{GREEDYDATA:extra_fields}");

        Doc doc = createDoc(field, APACHE_LOG_SAMPLE, "verb", "POST");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        config.put("overwrite", Arrays.asList("verb"));
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertApacheLog(doc, processResult);
        assertThat(doc.hasField("extra_fields")).isFalse();
    }

    @Test
    public void testWithoutOverwrite() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("%{WORD:http-verb}");

        Doc doc = createDoc(field, "GET", "http-verb", "POST");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((List)doc.getField("http-verb")).isEqualTo(Arrays.asList("POST", "GET"));
    }

    @Test
    public void testUnknownConversionType() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("%{WORD:verb:sheker}");

        Doc doc = createDoc(field, "GET");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField("verb_grokfailure")).isFalse();
        assertThat((String)doc.getField("verb")).isEqualTo("GET");
    }

    @Test
    public void testCustomPatterns() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("(?<custompattern>\\w+)");

        Doc doc = createDoc(field, "message");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("custompattern")).isEqualTo("message");
    }

    @Test
    public void testCustomPatternsWithHyphen() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("(?<custom-pattern>\\w+)");

        Doc doc = createDoc(field, "message");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("custom-pattern")).isEqualTo("message");
    }

    @Test
    public void testConverters() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("%{NUMBER:int:int} %{NUMBER:float:float} %{NUMBER:long:long} %{NUMBER:double:double}");

        Doc doc = createDoc(field, "200 15.5 200 15.5");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.getField("int").getClass()).isEqualTo(doc.getField("long").getClass()).isEqualTo(Long.class);
        assertThat(doc.getField("float").getClass()).isEqualTo(doc.getField("double").getClass()).isEqualTo(Double.class);
    }

    @Test
    public void testGrokParseFailure() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}+%{GREEDYDATA:extra_fields}");

        Doc doc = createDoc(field, "not apache log");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat((List)doc.getField("tags")).isEqualTo(Arrays.asList("_grokparsefailure"));
    }

    @Test
    public void testGrokParseFailureWithCustomTag() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList("%{COMBINEDAPACHELOG}+%{GREEDYDATA:extra_fields}");
        List<String> tagsOnFailure = Arrays.asList("parsedfailed :(", "reallysad");

        Doc doc = createDoc(field, "not apache log");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        config.put("tagsOnFailure", tagsOnFailure);
        GrokProcessor grokProcessor = factory.create(config);

        ProcessResult processResult = grokProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat((List)doc.getField("tags")).isEqualTo(tagsOnFailure);
    }

    @Test
    public void testIgnoreMissing() throws InterruptedException {
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
    public void testWithoutIgnoreMissing() throws InterruptedException {
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
    public void testConfigWithoutPatterns() throws InterruptedException {
        Map<String,Object> config = new HashMap<>();
        config.put("field", "someField");

        assertThatThrownBy(() -> factory.create(config)).isInstanceOf(ProcessorConfigurationException.class);
    }

    @Test
    public void testWithNonExistsPattern() throws InterruptedException {
        Map<String,Object> config = new HashMap<>();
        config.put("field", "someField");
        config.put("patterns", Arrays.asList("%{NONEXISTSPATTERN}"));

        assertThatThrownBy(() -> factory.create(config)).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testPatternsPriority() throws InterruptedException {
        String field = "message";
        List<String> patterns = Arrays.asList(
                "%{COMBINEDAPACHELOG}+%{GREEDYDATA:extra_fields}",
                "%{COMMONAPACHELOG}+%{GREEDYDATA:extra_fields}"
        );

        Doc doc1 = createDoc("message", "10.220.21.10 - - [26/Jan/2017:16:14:07 +0100] \"GET /clioonline.abo2/authentication/cliologin HTTP/1.0\" 404 238");
        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", patterns);
        config.put("ignoreMissing", false);
        GrokProcessor grokProcessor = factory.create(config);

        grokProcessor.process(doc1);

        Doc doc2 = createDoc("message", "194.239.185.67 - - [26/Jan/2017:14:32:46 +0100] \"GET /religionsfaget/udskoling/typo3temp/Assets/464cb9f3a6.css?1467394170 HTTP/1\" 200 1196 \"http://www.clioonline.dk/religionsfaget/udskoling/emner/religioner/islam/islams-trosgrundlag/\" \"Mozilla/5.0 (X11; CrOS armv7l 8872.76.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.105 Safari/537.36\"");
        grokProcessor.process(doc2);

        assertThat(doc2.hasField("extra_fields")).isFalse();
        assertThat(doc2.getField("httpversion").toString()).isEqualTo("1.0");
    }

    @Test
    public void testInvalidExpression() throws InterruptedException {
        String field = "message";
        List<String> invalidPatterns = Arrays.asList("%{COMBINEDAPACHELOG}+%[GREEDYDATA:extra_fields_with_wrong_bracket");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("patterns", invalidPatterns);
        config.put("ignoreMissing", false);
        assertThatThrownBy(() -> factory.create(config))
                .isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("Failed to create grok for expression");
    }

    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createProcessor(GrokProcessor.class, "patterns", Arrays.asList("pattern"))).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testInterruptStopsProcessor() {
        String field = "message";
        List<String> patterns = IntStream.range(1, 10000).mapToObj(i -> "%{COMBINEDAPACHELOG}").collect(Collectors.toList());
        Doc doc = createDoc(field, RandomStringUtils.randomAlphanumeric(100000));

        Map<String,Object> config = ImmutableMap.of(
                "field", field,
                "patterns", patterns
        );
        GrokProcessor grokProcessor = factory.create(config);

        interruptCurrentThreadIn(100);
        assertThatThrownBy(() -> grokProcessor.process(doc))
                .isInstanceOf(InterruptedException.class);
    }

    private void interruptCurrentThreadIn(long millis) {
        Thread currentThread = Thread.currentThread();
        ScheduledExecutorService interrupter = Executors.newScheduledThreadPool(1);
        interrupter.schedule(currentThread::interrupt, millis, MILLISECONDS);
    }
}
