package io.logz.sawmill.processors;

import com.google.common.collect.Sets;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.ListUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class DocSignatureProcessorTest {

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

    private final String DOC_SIGNATURE_FIELD = "logzio_doc_signature";
    private final Set<String> includeValueFields = Stream.of("logger", "line").collect(Collectors.toSet());
    private final Set<String> missingFields = Stream.of("missingField").collect(Collectors.toSet());

    @Test
    public void testFieldsNamesSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_NAMES);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        String expectedSignature = JsonUtils.toJsonString(fieldsNames);
        assertThat(signature).isEqualTo(expectedSignature.hashCode());
    }

    @Test
    public void testFieldsValuesSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("includeValueFields", includeValueFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        String expectedSignature = JsonUtils.toJsonString(getFieldsValues(doc, includeValueFields));
        assertThat(signature).isEqualTo(expectedSignature.hashCode());
    }

    @Test
    public void testHybridSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.HYBRID);
        config.put("includeValueFields", includeValueFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        List<String> values = getFieldsValues(doc, includeValueFields);
        String expectedSignature = JsonUtils.toJsonString(values) + JsonUtils.toJsonString(fieldsNames);
        assertThat(signature).isEqualTo(expectedSignature.hashCode());
    }

    @Test
    public void testFieldsNamesEmptySignature() throws InterruptedException {
        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class);
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        Doc doc = new Doc(map);
        doc.removeField("key");

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isFalse();
    }

    @Test
    public void testFieldsValuesEmptySignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("includeValueFields", missingFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isFalse();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isFalse();
    }

    @Test
    public void testFieldsValuesSignatureMissingFields() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("includeValueFields", Sets.union(includeValueFields, missingFields));

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        String expectedSignature = JsonUtils.toJsonString(
                getFieldsValues(doc, Sets.union(includeValueFields, missingFields)));
        assertThat(signature).isEqualTo(expectedSignature.hashCode());
    }

    @Test
    public void testFieldsValuesSignatureAddedField() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("includeValueFields", includeValueFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult firstResult = fieldsNamesSignatureProcessor.process(doc);

        assertThat(firstResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int firstSignature = doc.getField(DOC_SIGNATURE_FIELD);

        Set<String> newIncludeValueFields = new HashSet<>(includeValueFields);
        newIncludeValueFields.add("loglevel");
        config.put("includeValueFields", newIncludeValueFields);

        DocSignatureProcessor postAdditionProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        doc.removeField(DOC_SIGNATURE_FIELD);

        ProcessResult secondResult = postAdditionProcessor.process(doc);
        assertThat(secondResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int secondSignature = doc.getField(DOC_SIGNATURE_FIELD);
        assertThat(firstSignature).isNotEqualTo(secondSignature);
    }

    @Test
    public void testExecutionTime() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_NAMES);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);

        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        List<Long> executions = new ArrayList<>();

        ProcessResult result = null;
        for(int i=0; i<5000; i++) {
            Instant start = Instant.now();
            result = fieldsNamesSignatureProcessor.process(doc);
            assertThat(result.isSucceeded()).isTrue();
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            executions.add(timeElapsed);
        }
        System.out.println("execution avg: " + getAvg(executions));
    }

    public double getAvg(List<Long> executions) {
        return executions.stream()
                .mapToDouble(d -> d)
                .average()
                .orElse(0.0);
    }
    private List<String> getFieldsValues(Doc doc, Set<String> includeValueFields) {
        return includeValueFields.stream().collect(Collectors.toList())
                .stream()
                .filter(doc::hasField)
                .map(doc::getField)
                .map(Object::toString)
                .collect(Collectors.toList());
    }
}
