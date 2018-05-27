package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Template;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.TemplateService.JSON_STRING_SUFFIX;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static io.logz.sawmill.utils.FactoryUtils.templateService;
import static java.util.Collections.EMPTY_MAP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AddAndRemoveFieldProcessorTest {

    @Test
    public void testAddAndRemoveField() {
        String path = "message.hola.hello";
        AddFieldProcessor addFieldProcessor = createProcessor(AddFieldProcessor.class, "path", path, "value", "shalom");
        RemoveFieldProcessor removeFieldProcessor = createProcessor(RemoveFieldProcessor.class, "path", path);

        Doc doc = createDoc("field", "value");

        assertThat(addFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String) doc.getField(path)).isEqualTo("shalom");

        assertThat(removeFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThatThrownBy(() ->  doc.getField(path)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRemoveNonExistsField() {
        String path = "message.hola.hello";
        RemoveFieldProcessor removeFieldProcessor = createProcessor(RemoveFieldProcessor.class, "path", path);

        Doc doc = createDoc("field", "value");

        assertThat(removeFieldProcessor.process(doc).isSucceeded()).isTrue();
    }

    @Test
    public void testAddAndRemoveFieldWithTemplate() {
        String path = "message{{field}}";
        AddFieldProcessor addFieldProcessor = createProcessor(AddFieldProcessor.class, "path", path, "value", "{{objectField" + JSON_STRING_SUFFIX + "}}");
        RemoveFieldProcessor removeFieldProcessor = createProcessor(RemoveFieldProcessor.class, "path", path);

        Doc doc = createDoc("field", "Hola",
                "objectField", ImmutableMap.of("innerKey", "innerValue"));

        assertThat(addFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String) doc.getField("messageHola")).isEqualTo("{\"innerKey\":\"innerValue\"}");

        assertThat(removeFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThatThrownBy(() ->  doc.getField("messageHola")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRemoveSeveralFields() {
        List<String> fields = Arrays.asList("field1", "field2", "field3", "fieldThatDoesntExists", "field{{withTemplate}}");
        RemoveFieldProcessor removeFieldProcessor = createProcessor(RemoveFieldProcessor.class, "fields", fields);

        Doc doc = createDoc("field1", "value1",
                "field2", "value2",
                "field3", "value3",
                "field4", "value4",
                "withTemplate", "4");

        assertThat(removeFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().size()).isEqualTo(1);

        fields.forEach(field -> {
            Template template = templateService.createTemplate(field);
            assertThat(doc.hasField(template.render(doc))).isFalse();
        });
    }

    @Test
    public void testRemoveProcessorWithBadConfigs() {
        assertThatThrownBy(() -> createProcessor(RemoveFieldProcessor.class, EMPTY_MAP)).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(RemoveFieldProcessor.class, "fields", Arrays.asList("1", "2"), "path", "3")).isInstanceOf(ProcessorConfigurationException.class);
    }

    @Test
    public void testAddProcessorWithBadConfigs() {
        assertThatThrownBy(() -> createProcessor(AddFieldProcessor.class)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createProcessor(AddFieldProcessor.class, "path", "aaaa")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createProcessor(AddFieldProcessor.class, "value", "aaaa")).isInstanceOf(NullPointerException.class);
    }
}
