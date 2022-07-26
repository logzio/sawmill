package io.logz.sawmill.processors;

import com.google.common.collect.Sets;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AddSignatureProcessorTest {

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

    private final String SIGNATURE_FIELD_NAME = "logzio_doc_signature";
    private final Set<String> includeValueFields = Stream.of("logger", "line").collect(Collectors.toSet());
    private final Set<String> missingFields = Stream.of("missingField").collect(Collectors.toSet());

    @Test
    public void testFieldsNamesSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_NAMES);

        int expectedSignature = fieldsNames.hashCode();
        testSignature(config, expectedSignature);
    }

    @Test
    public void testFieldsValuesSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("includeValueFields", includeValueFields);

        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        int expectedSignature = getFieldsValues(doc, includeValueFields).hashCode();
        testSignature(config, expectedSignature);
    }

    @Test
    public void testHybridSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.HYBRID);
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("includeValueFields", includeValueFields);

        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        int expectedSignature = fieldsNames.hashCode() + getFieldsValues(doc, includeValueFields).hashCode();
        testSignature(config, expectedSignature);
    }

    @Test
    public void testFieldsNamesEmptySignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_NAMES);

        AddSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(AddSignatureProcessor.class, config);

        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        Doc doc = new Doc(map);
        doc.removeField("key");

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isFalse();
        assertThat(result.getError().isPresent()).isTrue();
        assertThat(result.getError().get().getMessage().contains("failed to extract fields names")).isTrue();
    }

    @Test
    public void testFieldsValuesEmptySignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("includeValueFields", missingFields);

        AddSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(AddSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isFalse();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isFalse();
        assertThat(result.getError().isPresent()).isTrue();
        assertThat(result.getError().get().getMessage().contains("failed to add signature field, signature collection is empty")).isTrue();
    }

    @Test
    public void testFieldsValuesSignatureMissingFields() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("includeValueFields", Sets.union(includeValueFields, missingFields));

        AddSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(AddSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isTrue();

        int signature = doc.getField(SIGNATURE_FIELD_NAME);
        int expectedSignature = getFieldsValues(doc, Sets.union(includeValueFields, missingFields)).hashCode();
        assertThat(signature).isEqualTo(expectedSignature);
    }

    @Test
    public void testFieldsValuesSignatureNestedField() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        Set<String> includeValueFields = Stream.of("nestedObject1.nestedObject2.field2").collect(Collectors.toSet());
        config.put("includeValueFields", includeValueFields);

        AddSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(AddSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isTrue();

        int signature = doc.getField(SIGNATURE_FIELD_NAME);
        int expectedSignature = getFieldsValues(doc, includeValueFields).hashCode();
        assertThat(signature).isEqualTo(expectedSignature);
    }

    @Test
    public void testMissingSignatureFieldName() {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_NAMES);
        config.put("signatureFieldName", "");
        config.put("includeValueFields", includeValueFields);

        assertThatThrownBy(() -> createProcessor(AddSignatureProcessor.class, config))
                .isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("signatureFieldName can not be empty");
    }

    @Test
    public void testFieldsValuesSignatureEmptyFieldsNames() {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("includeValueFields", Collections.emptySet());

        assertThatThrownBy(() -> createProcessor(AddSignatureProcessor.class, config))
                .isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("includeValueFields can not be empty");
    }

    @Test
    public void testFieldsNamesIdenticalSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_NAMES);

        int expectedSignature = fieldsNames.hashCode();
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        testLogProduceIdenticalSignature(config, expectedSignature, doc);
    }

    @Test
    public void testFieldsValuesIdenticalSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("includeValueFields", includeValueFields);

        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        int expectedSignature = getFieldsValues(doc, includeValueFields).hashCode();
        testLogProduceIdenticalSignature(config, expectedSignature, doc);
    }

    @Test
    public void testHybridIdenticalSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.HYBRID);
        config.put("includeValueFields", includeValueFields);

        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        int expectedSignature = fieldsNames.hashCode() + getFieldsValues(doc, includeValueFields).hashCode();
        testLogProduceIdenticalSignature(config, expectedSignature, doc);
    }

    @Test
    public void testUnorderedLogIdenticalSignature() throws InterruptedException {
        String msg = "{\"a\":\"b\",\"b\":\"c\"}";
        String unorderedMsg = "{\"b\":\"c\",\"a\":\"b\"}";

        Map<String, Object> config = new HashMap<>();
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_NAMES);

        AddSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(AddSignatureProcessor.class, config);

        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, msg));
        Doc unorderedDoc = new Doc(JsonUtils.fromJsonString(Map.class, unorderedMsg));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        ProcessResult unorderedDocResult = fieldsNamesSignatureProcessor.process(unorderedDoc);

        assertThat(result.isSucceeded()).isEqualTo(unorderedDocResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isEqualTo(unorderedDoc.hasField(SIGNATURE_FIELD_NAME)).isTrue();
        assertThat((int)doc.getField(SIGNATURE_FIELD_NAME)).isEqualTo(unorderedDoc.getField(SIGNATURE_FIELD_NAME));
    }

    @Test
    public void testUnorderedIncludeValueFieldsIdenticalSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", AddSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", SIGNATURE_FIELD_NAME);
        config.put("includeValueFields", new ArrayList<>(includeValueFields));

        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));
        int expectedSignature = getFieldsValues(doc, includeValueFields).hashCode();
        testSignature(config, expectedSignature);

        List<String> unorderedIncludeValueFields = Stream.of("line", "logger").collect(Collectors.toList());
        config.put("includeValueFields", unorderedIncludeValueFields);
        testSignature(config, expectedSignature);
    }

    private void testSignature(Map<String, Object> config, int expectedSignature) throws InterruptedException {
        AddSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(AddSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isTrue();

        int signature = doc.getField(SIGNATURE_FIELD_NAME);
        assertThat(signature).isEqualTo(expectedSignature);
    }

    private void testLogProduceIdenticalSignature(Map<String, Object> config, int expectedSignature, Doc doc)
            throws InterruptedException {
        AddSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(AddSignatureProcessor.class, config);

        ProcessResult firstResult = fieldsNamesSignatureProcessor.process(doc);

        assertThat(firstResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isTrue();

        int firstSignature = doc.getField(SIGNATURE_FIELD_NAME);
        assertThat(firstSignature).isEqualTo(expectedSignature);

        doc.removeField(SIGNATURE_FIELD_NAME);

        fieldsNamesSignatureProcessor = createProcessor(AddSignatureProcessor.class, config);
        ProcessResult secondResult = fieldsNamesSignatureProcessor.process(doc);

        assertThat(secondResult.isSucceeded()).isTrue();
        assertThat(doc.hasField(SIGNATURE_FIELD_NAME)).isTrue();

        int secondSignature = doc.getField(SIGNATURE_FIELD_NAME);
        assertThat(secondSignature).isEqualTo(firstSignature);
    }

    private Set<String> getFieldsValues(Doc doc, Set<String> includeValueFields) {
        return includeValueFields.stream()
                .filter(doc::hasField)
                .map(fieldName -> {
                    String value = doc.getField(fieldName);
                    return "value_" + fieldName + "_" + value;
                }).collect(Collectors.toSet());
    }
}
