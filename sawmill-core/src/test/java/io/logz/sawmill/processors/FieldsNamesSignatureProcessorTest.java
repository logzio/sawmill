package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class FieldsNamesSignatureProcessorTest {

    private final String message = "{\n" +
            "\t\"hostname\": \"log-analytics-56fc86d6f7-bpg4g\",\n" +
            "\t\"@timestamp\": \"2022-07-14T14:00:02.766Z\",\n" +
            "\t\"LogSize\": 730,\n" +
            "\t\"line\": \"133\",\n" +
            "\t\"logger\": \"io.logz.taskz.core.TaskzExecutor\",\n" +
            "\t\"loglevel\": \"INFO\",\n" +
            "\t\"pod-name\": \"log-analytics-56fc86d6f7-bpg4g\",\n" +
            "\t\"thread\": \"default-task-pool-50\",\n" +
            "\t\"message\": \"something happened\",\n" +
            "\t\"region\": \"us-east-1\",\n" +
            "\t\"listOfMaps\": [{\n" +
            "\t\t\"key1\": \"value1\",\n" +
            "\t\t\"innerMap1\": {\n" +
            "\t\t\t\"innerKey1\": \"innerValue1\"\n" +
            "\t\t},\n" +
            "\t\t\"key\": \"value\"\n" +
            "\t}, {\n" +
            "\t\t\"key1\": \"value1\",\n" +
            "\t\t\"innerMap2\": {\n" +
            "\t\t\t\"innerKey2\": \"innerValue2\"\n" +
            "\t\t},\n" +
            "\t\t\"key\": \"value\"\n" +
            "\t}],\n" +
            "\t\"nestedObject1\": {\n" +
            "\t\t\"field1\": 123,\n" +
            "\t\t\"nestedObject2\": {\n" +
            "\t\t\t\"field2\": \"123\",\n" +
            "\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\"nestedObject3\": {\n" +
            "\t\t\t\t\"field3\": \"poasdsa\",\n" +
            "\t\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\t\"introduced_in\": \"v0.10.0\"\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}";

    private final Set<String> fieldsNames = Stream.of(
            "nestedObject1.field1",
                    "line",
                    "listOfMaps.key1",
                    "logger",
                    "listOfMaps.innerMap2.innerKey2",
                    "thread",
                    "message",
                    "nestedObject1.nestedObject2.field2",
                    "hostname",
                    "nestedObject1.nestedObject2.displayName",
                    "@timestamp",
                    "LogSize",
                    "nestedObject1.nestedObject2.nestedObject3.name",
                    "listOfMaps.key",
                    "loglevel",
                    "nestedObject1.nestedObject2.nestedObject3.introduced_in",
                    "nestedObject1.nestedObject2.nestedObject3.field3",
                    "pod-name",
                    "region",
                    "listOfMaps.innerMap1.innerKey1",
                    "nestedObject1.nestedObject2.nestedObject3.displayName",
                    "nestedObject1.nestedObject2.name")
            .collect(Collectors.toSet());

    private final String FIELDS_NAMES_SIGNATURE = "logzio_fields_signature";

    @Test
    public void testFieldsNamesSignature() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class, "includeTypeFieldInSignature", "true");
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(FIELDS_NAMES_SIGNATURE)).isTrue();
        int signature = doc.getField(FIELDS_NAMES_SIGNATURE);
        String type = doc.hasField("type") ? doc.getField("type") : "";
        assertThat(signature).isEqualTo((type + JsonUtils.toJsonString(fieldsNames)).hashCode());
    }

    @Test
    public void testFieldsNamesSignatureWithoutTypeFieldInMessage() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        doc.removeField("type");
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(FIELDS_NAMES_SIGNATURE)).isTrue();
        int signature = doc.getField(FIELDS_NAMES_SIGNATURE);
        fieldsNames.remove("type");
        assertThat(signature).isEqualTo((JsonUtils.toJsonString(fieldsNames)).hashCode());
    }

    @Test
    public void testFieldsNamesSignatureWithoutTypeFieldValueInSignature() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(FIELDS_NAMES_SIGNATURE)).isTrue();
        int signature = doc.getField(FIELDS_NAMES_SIGNATURE);
        assertThat(signature).isEqualTo(JsonUtils.toJsonString(fieldsNames).hashCode());
    }

    @Test
    public void testEmptyDoc() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class);
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        Doc doc = new Doc(map);
        doc.removeField("key");
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(FIELDS_NAMES_SIGNATURE)).isTrue();
        int signature = doc.getField(FIELDS_NAMES_SIGNATURE);
        assertThat(signature).isEqualTo(JsonUtils.toJsonString(Collections.emptySet()).hashCode());
    }
}
