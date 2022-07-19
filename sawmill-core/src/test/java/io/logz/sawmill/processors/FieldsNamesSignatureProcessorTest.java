package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

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
            "\t\"message\": \"started async task TaskExecution(executionId=AddNewAccountLogTypesTask-1971802461--791174232-88265380-8, taskKey=TaskKey(name=AddNewAccountLogTypesTask, group=es-logs-us-east-1d-prod-27, subGroup=default), params={clusterId=es-logs-us-east-1d-prod-27, addAccountLogTypesFromSecondsAgo=3900}, retryCount=0) on TaskKey(name=AddNewAccountLogTypesTask, group=es-logs-us-east-1d-prod-27, subGroup=default)\",\n" +
            "\t\"type\": \"log-analytics\",\n" +
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
            "\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\"name\": \"fs\",\n" +
            "\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\"stability\": 2,\n" +
            "\t\t\"stabilityText\": \"Stable\",\n" +
            "\t\t\"nestedObject2\": {\n" +
            "\t\t\t\"field2\": \"123\",\n" +
            "\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\t\"stability\": 2,\n" +
            "\t\t\t\"stabilityText\": \"Stable\",\n" +
            "\t\t\t\"nestedObject3\": {\n" +
            "\t\t\t\t\"field3\": \"poasdsa\",\n" +
            "\t\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\t\t\"stability\": 2,\n" +
            "\t\t\t\t\"stabilityText\": \"Stable\",\n" +
            "\t\t\t\t\"nestedObject4\": {\n" +
            "\t\t\t\t\t\"field4\": \"12\",\n" +
            "\t\t\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\t\t\t\"stability\": 2,\n" +
            "\t\t\t\t\t\"stabilityText\": \"Stable\",\n" +
            "\t\t\t\t\t\"nestedObject5\": {\n" +
            "\t\t\t\t\t\t\"field5\": \"12\",\n" +
            "\t\t\t\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\t\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\t\t\t\t\"stability\": 2,\n" +
            "\t\t\t\t\t\t\"stabilityText\": \"Stable\"\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}";


    private final Set<String> fieldsNames = Stream.of(
            "nestedObject1.nestedObject2.nestedObject3.nestedObject4.field4",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.stability",
                    "line",
                    "listOfMaps.key1",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.nestedObject5.introduced_in",
                    "logger",
                    "listOfMaps.innerMap2.innerKey2",
                    "nestedObject1.nestedObject2.stability",
                    "nestedObject1.nestedObject2.stabilityText",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.nestedObject5.name",
                    "type",
                    "nestedObject1.nestedObject2.nestedObject3.stabilityText",
                    "nestedObject1.nestedObject2.field2",
                    "hostname",
                    "nestedObject1.nestedObject2.displayName",
                    "LogSize",
                    "listOfMaps.key",
                    "nestedObject1.nestedObject2.nestedObject3.introduced_in",
                    "nestedObject1.stability","listOfMaps.innerMap1.innerKey1",
                    "nestedObject1.nestedObject2.nestedObject3.displayName",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.nestedObject5.displayName",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.name",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.introduced_in",
                    "nestedObject1.name",
                    "nestedObject1.field1",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.displayName",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.nestedObject5.stability",
                    "thread",
                    "message",
                    "nestedObject1.stabilityText",
                    "nestedObject1.introduced_in",
                    "@timestamp",
                    "nestedObject1.nestedObject2.introduced_in",
                    "nestedObject1.nestedObject2.nestedObject3.name",
                    "nestedObject1.nestedObject2.nestedObject3.stability",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.nestedObject5.field5",
                    "loglevel","nestedObject1.displayName","nestedObject1.nestedObject2.nestedObject3.field3",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.stabilityText",
                    "pod-name",
                    "region",
                    "nestedObject1.nestedObject2.nestedObject3.nestedObject4.nestedObject5.stabilityText",
                    "nestedObject1.nestedObject2.name")
            .collect(Collectors.toSet());

    @Test
    public void testFieldsNamesSignature() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class, "includeTypeFieldInSignature", "true");
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField("logzio_fields_signature")).isTrue();
        int signature = doc.getField("logzio_fields_signature");
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
        assertThat(doc.hasField("logzio_fields_signature")).isTrue();
        int signature = doc.getField("logzio_fields_signature");
        fieldsNames.remove("type");
        assertThat(signature).isEqualTo((JsonUtils.toJsonString(fieldsNames)).hashCode());
    }

    @Test
    public void testFieldsNamesSignatureWithoutTypeField() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField("logzio_fields_signature")).isTrue();
        int signature = doc.getField("logzio_fields_signature");
        assertThat(signature).isEqualTo(JsonUtils.toJsonString(fieldsNames).hashCode());
    }
}
