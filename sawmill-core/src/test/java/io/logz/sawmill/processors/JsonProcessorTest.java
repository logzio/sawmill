package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonProcessorTest {

    public static final String VALID_JSON = "{" +
            "   \"field1\":\"value\"," +
            "   \"map1\": {" +
            "       \"field2\": 10," +
            "       \"field3\": \"value\"" +
            "   }," +
            "   \"list1\": [\"string1\",\"string2\"]," +
            "   \"field2\": \"valueWith\\Backslashes\"" +
            "}";
    public static final String INVALID_JSON = "{ this is: invalid json }";
    public static final String VALID_JSON_WITH_MESSAGE_FIELD = "{" +
            "   \"message\":\"value\"," +
            "   \"map1\": {" +
            "       \"field2\": 10," +
            "       \"field3\": \"value\"" +
            "   }," +
            "   \"list1\": [\"string1\",\"string2\"]" +
            "}";;

    @Test
    public void testValidJsonWithTarget() {
        String field = "message";
        String targetField = "json";

        Map jsonMap = JsonUtils.fromJsonString(Map.class, VALID_JSON);

        Doc doc = createDoc(field, VALID_JSON);

        JsonProcessor jsonProcessor = createProcessor(JsonProcessor.class, createConfig("field", field, "targetField", targetField));

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Map) doc.getField(targetField)).isEqualTo(jsonMap);
        assertThat(doc.hasField(field)).isFalse();
    }

    @Test
    public void testValidJsonWithTemplateTarget() {
        String field = "message";
        String targetField = "{{jsonField}}";

        Map jsonMap = JsonUtils.fromJsonString(Map.class, VALID_JSON);

        Doc doc = createDoc(field, VALID_JSON,
                "jsonField", "json");

        JsonProcessor jsonProcessor = createProcessor(JsonProcessor.class, createConfig("field", field, "targetField", targetField));

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Map) doc.getField("json")).isEqualTo(jsonMap);
        assertThat(doc.hasField(field)).isFalse();
    }

    @Test
    public void testValidJsonWithoutTarget() {
        String field = "message";

        Map<String,Object> jsonMap = JsonUtils.fromJsonString(Map.class, VALID_JSON);

        Doc doc = createDoc(field, VALID_JSON);

        JsonProcessor jsonProcessor = createProcessor(JsonProcessor.class, createConfig("field", field));

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        jsonMap.entrySet().forEach(entry ->
            assertThat((Object) doc.getField(entry.getKey())).isEqualTo(entry.getValue())
        );
        assertThat(doc.hasField(field)).isFalse();
    }

    @Test
    public void testValidJsonOverrideSourceField() {
        String field = "message";

        Map<String,Object> jsonMap = JsonUtils.fromJsonString(Map.class, VALID_JSON_WITH_MESSAGE_FIELD);

        Doc doc = createDoc(field, VALID_JSON_WITH_MESSAGE_FIELD);

        JsonProcessor jsonProcessor = createProcessor(JsonProcessor.class, createConfig("field", field));

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(field)).isTrue();
        jsonMap.entrySet().forEach(entry -> {
            assertThat((Object) doc.getField(entry.getKey())).isEqualTo(entry.getValue());
        });
    }

    @Test
    public void testJsonParseFailure() {
        String field = "message";

        Doc doc = createDoc(field, INVALID_JSON);

        JsonProcessor jsonProcessor = createProcessor(JsonProcessor.class, createConfig("field", field));

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        assertThat((List)doc.getField("tags")).isEqualTo(Arrays.asList("_jsonparsefailure"));
        assertThat(doc.hasField(field)).isTrue();
    }

    @Test
    public void testFieldNotExists() {
        String fieldNotExists = "fieldNotExists";

        Doc doc = createDoc("message", VALID_JSON);

        JsonProcessor jsonProcessor = createProcessor(JsonProcessor.class, createConfig("field", fieldNotExists));

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createProcessor(JsonProcessor.class)).isInstanceOf(NullPointerException.class);
    }
}
