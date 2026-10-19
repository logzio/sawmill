package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KeyValueProcessorTest {

    public static final String KEV_VALUE_MESSAGE_TEMPLATE = "this is key-value message, the key-values start from here " +
            "simple{1}value{0}" +
            "brackets{1}[with space] {0}" +
            "  roundBrackets  {1} (with two spaces) {0}" +
            "angleBrackets {1} <without> {0}" +
            "%trim%{1}!value!{0}" +
            "complex {1}\"innerKey{1}innerValue withBrackets{1}(another innerValue)\"";
    public static final String KEY_VALUE_MESSAGE_WITH_DUPLICATE_KEYS = "this is KV with duplicate keys sameKey=value1 sameKey=value2 sameKey=value3 sameKey=value4";

    @Test
    public void testDefault() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = createConfig("field", field);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("simple")).isEqualTo("value");
        assertThat((String) doc.getField("brackets")).isEqualTo("with space");
        assertThat((String) doc.getField("roundBrackets")).isEqualTo("with two spaces");
        assertThat((String) doc.getField("angleBrackets")).isEqualTo("without");
        assertThat((String) doc.getField("%trim%")).isEqualTo("!value!");
        assertThat((String) doc.getField("complex")).isEqualTo("innerKey=innerValue withBrackets=(another innerValue)");
    }

    @Test
    public void testNonKeyValues() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, "this message is with out any kv key=");

        Map<String,Object> config = createConfig("field", field);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        Map<String, Object> originalSource = doc.getSource();

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.getSource()).isEqualTo(originalSource);
    }

    @Test
    public void testFieldDoesntExists() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc("differentField", "this message is with out any kv");

        Map<String,Object> config = createConfig("field", field);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testUnsupportedTypeField() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, 15);

        Map<String,Object> config = createConfig("field", field);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testNullField() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, null);

        Map<String,Object> config = createConfig("field", field);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testWithTargetField() throws InterruptedException {
        String field = "message";
        String targetField = "kv";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(targetField)).isTrue();
        Map<Object,String> kv = doc.getField(targetField);
        assertThat(kv.get("simple")).isEqualTo("value");
        assertThat(kv.get("brackets")).isEqualTo("with space");
        assertThat(kv.get("roundBrackets")).isEqualTo("with two spaces");
        assertThat(kv.get("angleBrackets")).isEqualTo("without");
        assertThat(kv.get("%trim%")).isEqualTo("!value!");
        assertThat(kv.get("complex")).isEqualTo("innerKey=innerValue withBrackets=(another innerValue)");
    }

    @Test
    public void testWithTemplateTargetField() throws InterruptedException {
        String field = "message";
        String targetField = "{{kvField}}";
        Doc doc = createDoc(field, getDefaultMessage(), "kvField", "kv");

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField("kv")).isTrue();
        Map<Object,String> kv = doc.getField("kv");
        assertThat(kv.get("simple")).isEqualTo("value");
        assertThat(kv.get("brackets")).isEqualTo("with space");
        assertThat(kv.get("roundBrackets")).isEqualTo("with two spaces");
        assertThat(kv.get("angleBrackets")).isEqualTo("without");
        assertThat(kv.get("%trim%")).isEqualTo("!value!");
        assertThat(kv.get("complex")).isEqualTo("innerKey=innerValue withBrackets=(another innerValue)");
    }

    @Test
    public void testRecursive() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = createConfig("field", field,
                "recursive", true);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("simple")).isEqualTo("value");
        assertThat((String) doc.getField("brackets")).isEqualTo("with space");
        assertThat((String) doc.getField("roundBrackets")).isEqualTo("with two spaces");
        assertThat((String) doc.getField("angleBrackets")).isEqualTo("without");
        assertThat((String) doc.getField("%trim%")).isEqualTo("!value!");
        assertThat(doc.hasField("complex")).isTrue();
        Map<String, Object> complexField = doc.getField("complex");
        assertThat(complexField.get("innerKey")).isEqualTo("innerValue");
        assertThat(complexField.get("withBrackets")).isEqualTo("another innerValue");
    }

    @Test
    public void testTrimsAndPrefix() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = createConfig("field", field,
                "trimKey", "%",
                "trim", "!",
                "prefix", "KV");

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("KVsimple")).isEqualTo("value");
        assertThat((String) doc.getField("KVbrackets")).isEqualTo("with space");
        assertThat((String) doc.getField("KVroundBrackets")).isEqualTo("with two spaces");
        assertThat((String) doc.getField("KVangleBrackets")).isEqualTo("without");
        assertThat((String) doc.getField("KVtrim")).isEqualTo("value");
        assertThat((String) doc.getField("KVcomplex")).isEqualTo("innerKey=innerValue withBrackets=(another innerValue)");
    }

    @Test
    public void testIncludeKeys() throws InterruptedException {
        String field = "message";

        String fieldSplit = ",";
        String valueSplit = "~";
        Doc doc = createDoc(field, getMessage(fieldSplit, valueSplit));

        Map<String,Object> config = createConfig("field", field,
                "fieldSplit", fieldSplit,
                "valueSplit", valueSplit,
                "includeKeys", Arrays.asList("brackets", "roundBrackets"));

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField("simple")).isFalse();
        assertThat((String) doc.getField("brackets")).isEqualTo("with space");
        assertThat((String) doc.getField("roundBrackets")).isEqualTo("with two spaces");
        assertThat(doc.hasField("angleBrackets")).isFalse();
        assertThat(doc.hasField("%trim%")).isFalse();
        assertThat(doc.hasField("complex")).isFalse();
    }

    @Test
    public void testExcludeKeys() throws InterruptedException {
        String field = "message";

        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = createConfig("field", field,
                "trimKey", "%",
                "trim", "!",
                "excludeKeys", Arrays.asList("brackets", "complex"));

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("simple")).isEqualTo("value");
        assertThat(doc.hasField("brackets")).isFalse();
        assertThat((String) doc.getField("roundBrackets")).isEqualTo("with two spaces");
        assertThat((String) doc.getField("angleBrackets")).isEqualTo("without");
        assertThat((String) doc.getField("trim")).isEqualTo("value");
        assertThat(doc.hasField("complex")).isFalse();
    }

    @Test
    public void testValueAsList() throws InterruptedException {
        String field = "message";

        Doc doc = createDoc(field, Arrays.asList(getDefaultMessage(), "anotherKV=anotherMagic"));

        Map<String,Object> config = createConfig("field", field);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("simple")).isEqualTo("value");
        assertThat((String) doc.getField("brackets")).isEqualTo("with space");
        assertThat((String) doc.getField("roundBrackets")).isEqualTo("with two spaces");
        assertThat((String) doc.getField("angleBrackets")).isEqualTo("without");
        assertThat((String) doc.getField("anotherKV")).isEqualTo("anotherMagic");
        assertThat((String) doc.getField("%trim%")).isEqualTo("!value!");
        assertThat((String) doc.getField("complex")).isEqualTo("innerKey=innerValue withBrackets=(another innerValue)");
    }

    @Test
    public void testAllowDuplicateValues() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, KEY_VALUE_MESSAGE_WITH_DUPLICATE_KEYS);

        Map<String,Object> config = createConfig("field", field);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((List) doc.getField("sameKey")).isEqualTo(Arrays.asList("value1", "value2", "value3", "value4"));
    }

    @Test
    public void testDontAllowDuplicateValues() throws InterruptedException {
        String field = "message";
        Doc doc = createDoc(field, KEY_VALUE_MESSAGE_WITH_DUPLICATE_KEYS);

        Map<String,Object> config = createConfig("field", field,
                "allowDuplicateValues", false);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("sameKey")).isEqualTo("value1");
    }

    @Test
    public void testMaxKeyLength() throws InterruptedException {
        int maxKeyLength = 80;
        String field = "message";
        String key = "thisShouldBeIgnored" + RandomStringUtils.randomAlphabetic(maxKeyLength) ;
        Doc doc = createDoc(field, Arrays.asList(getDefaultMessage(), key + "=anotherMagic"));

        Map<String,Object> config = createConfig("field", field, "maxKeyLength", maxKeyLength);

        KeyValueProcessor kvProcessor = createProcessor(KeyValueProcessor.class, config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(key)).isFalse();
    }


    private String getDefaultMessage() {
        return getMessage(" ", "=");
    }

    private String getMessage(String fieldSplit, String valueSplit) {
        return MessageFormat.format(KEV_VALUE_MESSAGE_TEMPLATE, fieldSplit, valueSplit);
    }

    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createProcessor(KeyValueProcessor.class)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testDuplicateKeysKeepMapAndOrder() throws InterruptedException {
        java.util.Random random = new java.util.Random(23);
        KeyValueProcessor processor = new KeyValueProcessor("kv", templateFor("out"), null, null, " ", "=",
                true, true, "", false, null, null, 40);
        for (int n = 0; n < 3000; n++) {
            // 1-70 pairs from a key pool sized to cross HashMap's resize thresholds with and without duplicates
            int pairs = 1 + random.nextInt(70);
            int keyPool = 1 + random.nextInt(80);
            StringBuilder text = new StringBuilder();
            Map<String, Object> expected = new java.util.HashMap<>();
            for (int i = 0; i < pairs; i++) {
                String key = "k" + random.nextInt(keyPool);
                String value = "v" + i;
                text.append(key).append('=').append(value).append(' ');
                // the previous implementation
                expected.compute(key, (k, oldVal) -> {
                    if (oldVal == null) return value;
                    if (oldVal instanceof List) {
                        ((List) oldVal).add(value);
                        return oldVal;
                    }
                    return new java.util.ArrayList<>(Arrays.asList(oldVal, value));
                });
            }
            Doc doc = createDoc("kv", text.toString());
            assertThat(processor.process(doc).isSucceeded()).isTrue();
            Map<String, Object> actual = doc.getField("out");
            assertThat(new java.util.ArrayList<>(actual.entrySet())).as("[%s]", text).isEqualTo(new java.util.ArrayList<>(expected.entrySet()));
        }
    }

    @Test
    public void testKeyPrefixes() throws InterruptedException {
        for (String prefix : new String[]{"", "p_", null}) {
            KeyValueProcessor processor = new KeyValueProcessor("kv", null, null, null, " ", "=",
                    true, true, prefix, false, null, null, 40);
            Doc doc = createDoc("kv", "a=1  b = 2");
            assertThat(processor.process(doc).isSucceeded()).isTrue();
            assertThat((String) doc.getField(prefix + "a")).isEqualTo("1");
            assertThat((String) doc.getField(prefix + "b")).isEqualTo("2");
        }
    }

    private static io.logz.sawmill.Template templateFor(String value) {
        return new io.logz.sawmill.TemplateService().createTemplate(value);
    }
}
