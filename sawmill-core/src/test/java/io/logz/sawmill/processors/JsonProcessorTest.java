package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class JsonProcessorTest {

    public static final String VALID_JSON = "{" +
            "   \"field1\":\"value\"," +
            "   \"map1\": {" +
            "       \"field2\": 10," +
            "       \"field3\": \"value\"" +
            "   }," +
            "   \"list1\": [\"string1\",\"string2\"]" +
            "}";
    public static final String INVALID_JSON = "{ this is: invalid json }";

    @Test
    public void testValidJsonWithTarget() {
        String field = "message";
        String targetField = "json";

        Map jsonMap = JsonUtils.fromJsonString(Map.class, VALID_JSON);

        Doc doc = createDoc(field, VALID_JSON);

        JsonProcessor jsonProcessor = new JsonProcessor(field, targetField);

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Map) doc.getField(targetField)).isEqualTo(jsonMap);
    }

    @Test
    public void testValidJsonWithoutTarget() {
        String field = "message";

        Map<String,Object> jsonMap = JsonUtils.fromJsonString(Map.class, VALID_JSON);

        Doc doc = createDoc(field, VALID_JSON);

        JsonProcessor jsonProcessor = new JsonProcessor(field, null);

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        jsonMap.entrySet().forEach(entry -> {
            assertThat((Object) doc.getField(entry.getKey())).isEqualTo(entry.getValue());
        });
    }

    @Test
    public void testInvalidJson() {
        String field = "message";

        Doc doc = createDoc(field, INVALID_JSON);

        JsonProcessor jsonProcessor = new JsonProcessor(field, null);

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testFieldNotExists() {
        String fieldNotExists = "fieldNotExists";

        Doc doc = createDoc("message", VALID_JSON);

        JsonProcessor jsonProcessor = new JsonProcessor(fieldNotExists, null);

        ProcessResult processResult = jsonProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }
}
