package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class KeyValueProcessorTest {

    public static final String KEV_VALUE_MESSAGE_TEMPLATE = "this is key-value message, the key-values start from here " +
            "simple{1}value{0}" +
            "brackets{1}[with space] {0}" +
            "  roundBrackets  {1} (with two spaces) {0}" +
            "angleBrackets {1} <without> {0}" +
            "%trim%{1}!value!{0}" +
            "complex {1}\"innerKey{1}innerValue withBrackets{1}(another innerValue)\"";
    public static final String KEY_VALUE_MESSAGE_WITH_DUPLICATE_KEYS = "this is KV with duplicate keys sameKey=value1 sameKey=value2 sameKey=value3 sameKey=value4";

    public static KeyValueProcessor.Factory factory;

    @BeforeClass
    public static void init() {
        factory = new KeyValueProcessor.Factory();
    }

    @Test
    public void testDefault() {
        String field = "message";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);

        KeyValueProcessor kvProcessor = factory.create(config);

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
    public void testNonKeyValues() {
        String field = "message";
        Doc doc = createDoc(field, "this message is with out any kv");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);

        KeyValueProcessor kvProcessor = factory.create(config);

        Map<String, Object> originalSource = doc.getSource();

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.getSource()).isEqualTo(originalSource);
    }

    @Test
    public void testFieldDoesntExists() {
        String field = "message";
        Doc doc = createDoc("differentField", "this message is with out any kv");

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);

        KeyValueProcessor kvProcessor = factory.create(config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testUnsupportedTypeField() {
        String field = "message";
        Doc doc = createDoc(field, 15);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);

        KeyValueProcessor kvProcessor = factory.create(config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testWithTargetField() {
        String field = "message";
        String targetField = "kv";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("targetField", targetField);

        KeyValueProcessor kvProcessor = factory.create(config);

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
    public void testRecursive() {
        String field = "message";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("recursive", true);

        KeyValueProcessor kvProcessor = factory.create(config);

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
    public void testTrimsAndPrefix() {
        String field = "message";
        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("trimKey", "%");
        config.put("trim", "!");
        config.put("prefix", "KV");

        KeyValueProcessor kvProcessor = factory.create(config);

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
    public void testIncludeKeys() {
        String field = "message";

        String fieldSplit = ",";
        String valueSplit = "~";
        Doc doc = createDoc(field, getMessage(fieldSplit, valueSplit));

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("fieldSplit", fieldSplit);
        config.put("valueSplit", valueSplit);
        config.put("includeKeys", Arrays.asList("brackets", "roundBrackets"));

        KeyValueProcessor kvProcessor = factory.create(config);

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
    public void testExcludeKeys() {
        String field = "message";

        Doc doc = createDoc(field, getDefaultMessage());

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("trimKey", "%");
        config.put("trim", "!");
        config.put("excludeKeys", Arrays.asList("brackets", "complex"));

        KeyValueProcessor kvProcessor = factory.create(config);

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
    public void testValueAsList() {
        String field = "message";

        Doc doc = createDoc(field, Arrays.asList(getDefaultMessage(), "anotherKV=anotherMagic"));

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);

        KeyValueProcessor kvProcessor = factory.create(config);

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
    public void testAllowDuplicateValues() {
        String field = "message";
        Doc doc = createDoc(field, KEY_VALUE_MESSAGE_WITH_DUPLICATE_KEYS);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);

        KeyValueProcessor kvProcessor = factory.create(config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((List) doc.getField("sameKey")).isEqualTo(Arrays.asList("value1", "value2", "value3", "value4"));
    }

    @Test
    public void testDontAllowDuplicateValues() {
        String field = "message";
        Doc doc = createDoc(field, KEY_VALUE_MESSAGE_WITH_DUPLICATE_KEYS);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("allowDuplicateValues", false);

        KeyValueProcessor kvProcessor = factory.create(config);

        ProcessResult processResult = kvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("sameKey")).isEqualTo("value1");
    }

    private String getDefaultMessage() {
        return getMessage(" ", "=");
    }

    private String getMessage(String fieldSplit, String valueSplit) {
        return MessageFormat.format(KEV_VALUE_MESSAGE_TEMPLATE, fieldSplit, valueSplit);
    }
}
