package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.Map;
import java.util.stream.IntStream;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RenameFieldProcessorTest {
    @Test
    public void testRenameField() {
        String fromField = RandomStringUtils.randomAlphanumeric(5);
        String nestedToField = RandomStringUtils.randomAlphanumeric(5) + "." + RandomStringUtils.randomAlphanumeric(5);
        Doc doc = createDoc(fromField, "value");

        Map<String, Object> config = createConfig("from", fromField,
                "to", nestedToField);
        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        assertThat(renameFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String)doc.getField(nestedToField)).isEqualTo("value");
        assertThatThrownBy(() -> doc.getField(fromField)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRenameNonExistsField() {
        String fromField = RandomStringUtils.randomAlphanumeric(5);
        String toField = RandomStringUtils.randomAlphanumeric(5);
        Doc doc = createDoc("differentField", "value");

        Map<String, Object> config = createConfig("from", fromField,
                "to", toField);
        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        assertThat(renameFieldProcessor.process(doc).isSucceeded()).isFalse();

        assertThatThrownBy(() -> doc.getField(toField)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRenameWithTemplateInFrom() {
        Doc doc = createDoc("field-a", "field-b",
                            "field-b", "value-of-c");

        Map<String, Object> config = createConfig(
                "from", "{{field-a}}",
                "to", "field-c");

        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        assertThat(renameFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String)doc.getField("field-c")).isEqualTo("value-of-c");
    }

    @Test
    public void testRenameWithTemplateInTo() {
        Doc doc = createDoc("field-a", "field-b",
                            "field-c", "value-of-c");

        Map<String, Object> config = createConfig(
                "from", "field-c",
                "to", "{{field-a}}");

        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        assertThat(renameFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String)doc.getField("field-b")).isEqualTo("value-of-c");
    }

    @Test
    public void testRenameJsonWithTemplateInTo() {
        Map valueBeingRename = ImmutableMap.of("x", 5);

        Doc doc = createDoc(
                "field-a", "field-b",
                "field-c", valueBeingRename);

        Map<String, Object> config = createConfig(
                "from", "field-c",
                "to", "{{field-a}}");

        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        assertThat(renameFieldProcessor.process(doc).isSucceeded()).isTrue();

        Map renamedValue = doc.getField("field-b");
        assertThat(renamedValue.get("x")).isEqualTo(5);
    }

    @Test
    public void testMultipleRenames() {
        Map<String, String> renames = ImmutableMap.of("fieldFrom1", "fieldTo1",
                "fieldFrom2", "{{field2}}",
                "{{field3}}", "fieldTo3",
                "{{field4}}", "{{field5}}");

        Doc doc = createDoc("fieldFrom1", "value1",
                "fieldFrom2", "value2",
                "fieldFrom3", "value3",
                "fieldFrom4", "value4",
                "fieldFrom5", "value5",
                "field2", "fieldTo2",
                "field3", "fieldFrom3",
                "field4", "fieldFrom4",
                "field5", "fieldTo4");

        Map<String, Object> config = createConfig(
                "from", "fieldFrom5",
                "to", "fieldTo5",
                "renames", renames);

        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        ProcessResult processResult = renameFieldProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        for (int i = 1; i <= 5; i++) {
            assertThat(doc.hasField("fieldTo" + i)).isTrue();
            assertThat((String) doc.getField("fieldTo" + i)).isEqualTo("value" + i);
            assertThat(doc.hasField("fieldFrom" + i)).isFalse();
        }
    }
}
