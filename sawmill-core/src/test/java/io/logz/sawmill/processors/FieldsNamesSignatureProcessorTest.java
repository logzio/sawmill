package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.Map;

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
            "\t\t\t\t\t\t\"stabilityText\": \"Stable\",\n" +
            "\t\t\t\t\t\t\"nestedObject6\": {\n" +
            "\t\t\t\t\t\t\t\"field6\": 12,\n" +
            "\t\t\t\t\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\t\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\t\t\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\t\t\t\t\t\"stability\": 2,\n" +
            "\t\t\t\t\t\t\t\"stabilityText\": \"Stable\",\n" +
            "\t\t\t\t\t\t\t\"nestedObject7\": {\n" +
            "\t\t\t\t\t\t\t\t\"field7\": \"daa\",\n" +
            "\t\t\t\t\t\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\t\t\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\t\t\t\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\t\t\t\t\t\t\"stability\": 2,\n" +
            "\t\t\t\t\t\t\t\t\"stabilityText\": \"Stable\",\n" +
            "\t\t\t\t\t\t\t\t\"nestedObject8\": {\n" +
            "\t\t\t\t\t\t\t\t\t\"field8\": \"32131\",\n" +
            "\t\t\t\t\t\t\t\t\t\"name\": \"fs\",\n" +
            "\t\t\t\t\t\t\t\t\t\"displayName\": \"Promise example\",\n" +
            "\t\t\t\t\t\t\t\t\t\"introduced_in\": \"v0.10.0\",\n" +
            "\t\t\t\t\t\t\t\t\t\"stability\": 2,\n" +
            "\t\t\t\t\t\t\t\t\t\"stabilityText\": \"Stable\"\n" +
            "\t\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t},\n" +
            "\t\"tags\": [\n" +
            "\t\t\"_logz_http_bulk_json_8070\"\n" +
            "\t]\n" +
            "}";


    @Test
    public void testFieldsNamesSignatureProcessor() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class, "includeTypeFieldInSignature", "true");
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
    }

    @Test
    public void testFieldsNamesSignatureProcessorWithoutTypeFieldValue() throws InterruptedException {
        FieldsNamesSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(FieldsNamesSignatureProcessor.class);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isTrue();
    }
}
