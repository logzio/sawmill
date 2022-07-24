package io.logz.sawmill.processors;

import com.google.common.collect.Sets;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_NAMES);


        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        int expectedSignature = new ArrayList<>(fieldsNames).hashCode();
        assertThat(signature).isEqualTo(expectedSignature);
    }

    @Test
    public void testFieldsValuesSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        config.put("includeValueFields", includeValueFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        int expectedSignature = getFieldsValues(doc, includeValueFields).hashCode();
        assertThat(signature).isEqualTo(expectedSignature);
    }

    @Test
    public void testHybridSignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.HYBRID);
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        config.put("includeValueFields", includeValueFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        List<String> values = getFieldsValues(doc, includeValueFields);
        int expectedSignature = Stream.concat(values.stream(), fieldsNames.stream())
                .collect(Collectors.toList()).hashCode();
        assertThat(signature).isEqualTo(expectedSignature);
    }

    @Test
    public void testFieldsNamesEmptySignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_NAMES);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);

        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        Doc doc = new Doc(map);
        doc.removeField("key");

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isFalse();
        assertThat(result.getError().isPresent()).isTrue();
        assertThat(result.getError().get().getMessage().contains("signature is empty")).isTrue();
    }

    @Test
    public void testFieldsValuesEmptySignature() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        config.put("includeValueFields", missingFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isFalse();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isFalse();
        assertThat(result.getError().isPresent()).isTrue();
        assertThat(result.getError().get().getMessage().contains("signature is empty")).isTrue();
    }

    @Test
    public void testFieldsValuesSignatureMissingFields() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        config.put("includeValueFields", Sets.union(includeValueFields, missingFields));

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        int expectedSignature = getFieldsValues(doc, Sets.union(includeValueFields, missingFields)).hashCode();
        assertThat(signature).isEqualTo(expectedSignature);
    }

    @Test
    public void testFieldsValuesSignatureAddedField() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
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
    public void testFieldsValuesSignatureNestedField() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        Set<String> includeValueFields = Stream.of("nestedObject1.nestedObject2.field2").collect(Collectors.toSet());
        config.put("includeValueFields", includeValueFields);

        DocSignatureProcessor fieldsNamesSignatureProcessor =
                createProcessor(DocSignatureProcessor.class, config);
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class, message));

        ProcessResult result = fieldsNamesSignatureProcessor.process(doc);

        assertThat(result.isSucceeded()).isTrue();
        assertThat(doc.hasField(DOC_SIGNATURE_FIELD)).isTrue();

        int signature = doc.getField(DOC_SIGNATURE_FIELD);
        int expectedSignature = getFieldsValues(doc, includeValueFields).hashCode();
        assertThat(signature).isEqualTo(expectedSignature);
    }

    @Test
    public void testMissingSignatureFieldName() throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_NAMES);
        config.put("signatureFieldName", "");
        config.put("includeValueFields", includeValueFields);

        assertThatThrownBy(() -> createProcessor(DocSignatureProcessor.class, config))
                .isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("signatureFieldName can not be empty");
    }

    @Test
    public void testFieldsValuesSignatureEmptyFieldsNames() {
        Map<String, Object> config = new HashMap<>();
        config.put("signatureMode", DocSignatureProcessor.SignatureMode.FIELDS_VALUES);
        config.put("signatureFieldName", DOC_SIGNATURE_FIELD);
        config.put("includeValueFields", Collections.emptySet());

        assertThatThrownBy(() -> createProcessor(DocSignatureProcessor.class, config))
                .isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("includeValueFields can not be empty");
    }

    private List<String> getFieldsValues(Doc doc, Set<String> includeValueFields) {
        return new ArrayList<>(includeValueFields)
                .stream()
                .filter(doc::hasField)
                .map(doc::getField)
                .map(Object::toString)
                .collect(Collectors.toList());
    }
}
