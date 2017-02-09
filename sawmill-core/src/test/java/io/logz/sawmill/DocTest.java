package io.logz.sawmill;

import com.google.common.collect.ImmutableMap;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class DocTest {

    public static Mustache.Compiler mustache = Mustache.compiler();

    @Test
    public void testAddGetAndRemoveFieldValue() {
        Doc doc = createDoc("message", "hola", "name", "test");

        String value = "shalom";
        String path = "object.nestedField";

        doc.addField(path, value);

        assertThat((String) doc.getField(path)).isEqualTo(value);

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveMapValue() {
        Doc doc = createDoc("message", "hola", "name", "test");

        Map value = ImmutableMap.of("jsonMap", ImmutableMap.of("nested", "a little"));
        String path = "object.nestedField";

        doc.addField(path, value);

        assertThat((Map) doc.getField(path)).isEqualTo(value);

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveNonJsonMapValue() {
        Doc doc = createDoc("message", "hola", "name", "test");

        Map value = ImmutableMap.of(ImmutableMap.of("crazy", "map"), ImmutableMap.of("really", "crazy"));
        String path = "object.nestedField";

        doc.addField(path, value);

        assertThat((Map) doc.getField(path)).isEqualTo(value);

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldWithStringTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";
        Doc doc = createDoc(nameField, "Naor",
                dayOfWeekField, "Shabat",
                someField, ".nested");
        Template path = TemplateService.compileTemplate("field{{" + someField + "}}");
        TemplatedValue value = TemplateService.compileValue("Hey {{" + nameField + "}}, {{" + dayOfWeekField + "}} is a beautiful day");

        doc.addField(path, value);

        assertThat(doc.hasField(path)).isTrue();
        assertThat((String) doc.getField(path)).isEqualTo("Hey Naor, Shabat is a beautiful day");

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldWithEmptyTemplate() {
        Doc doc = createDoc("someField", "value");
        Template path = TemplateService.compileTemplate("field_5");
        TemplatedValue valueTemplate = TemplateService.compileValue("Hey Naor, Shabat is a beautiful day");

        doc.addField(path, valueTemplate);

        assertThat(doc.hasField(path)).isTrue();
        assertThat((String) doc.getField(path)).isEqualTo("Hey Naor, Shabat is a beautiful day");

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldWithMapTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";
        String nestedField = "nestedField";
        Doc doc = createDoc(nameField, "Naor",
                dayOfWeekField, "Shabat",
                someField, "map",
                nestedField, "nested");
        Template path = TemplateService.compileTemplate("field_{{" + someField + "}}");
        TemplatedValue value = TemplateService.compileValue(
                ImmutableMap.of("{{" + nestedField + "}}", "Hey {{" + nameField + "}}, {{" + dayOfWeekField + "}} is a beautiful day",
                        "withList", Arrays.asList("{{" + nameField + "}}1", "{{" + nameField + "}}2"),
                        "withNumber", 2,
                        "withMap", ImmutableMap.of("key", "value")));

        doc.addField(path, value);

        Map<String, Object> valueAsMap = (Map<String, Object>) value.execute(doc.getSource());

        assertThat(doc.hasField(path)).isTrue();
        assertThat((Map) doc.getField(path)).isEqualTo(valueAsMap);

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldWithBooleanTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";
        Doc doc = createDoc(nameField, "Naor",
                dayOfWeekField, "Shabat",
                someField, 5);
        Template path = TemplateService.compileTemplate("field_{{" + someField + "}}");
        TemplatedValue value = TemplateService.compileValue(true);

        doc.addField(path, value);

        assertThat(doc.hasField(path)).isTrue();
        assertThat((Boolean) doc.getField(path)).isEqualTo(true);

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldWithNumberTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";
        Doc doc = createDoc(nameField, "Naor",
                dayOfWeekField, "Shabat",
                someField, 5);
        Template path = TemplateService.compileTemplate("field_{{" + someField + "}}");
        TemplatedValue value = TemplateService.compileValue(7);

        doc.addField(path, value);

        assertThat(doc.hasField(path)).isTrue();
        assertThat((Integer) doc.getField(path)).isEqualTo(7);

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldWithObjectTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";
        Object object = new Object() {
            int num = 6;
            String str = "custom";
        };
        Doc doc = createDoc(nameField, "Naor",
                dayOfWeekField, "Shabat",
                someField, 5);
        Template path = TemplateService.compileTemplate("field_{{" + someField + "}}");
        TemplatedValue value = TemplateService.compileValue(object);

        doc.addField(path, value);

        assertThat(doc.hasField(path)).isTrue();
        assertThat((Object) doc.getField(path)).isEqualTo(object);

        assertThat(doc.removeField(path)).isTrue();

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAppendAndRemoveFromList() {
        Doc doc = createDoc("message", "hola", "name", "test");

        String path = "list";
        List<String> value = Arrays.asList("value1", "value2");

        assertThat(doc.removeFromList(path,value)).isFalse();

        doc.appendList(path, value);

        for (String item : value) {
            assertThat(((List) doc.getField("list")).contains(item)).isTrue();
        }

        assertThat(doc.removeFromList(path, value)).isTrue();

        for (String item : value) {
            assertThat(((List) doc.getField("list")).contains(item)).isFalse();
        }
    }

    @Test
    public void testAppendAndRemoveFromListWithTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";
        Doc doc = createDoc(nameField, "Naor",
                dayOfWeekField, "Shabat",
                someField, 5);

        Template path = TemplateService.compileTemplate("list{{" + someField + "}}");
        TemplatedValue value = TemplateService.compileValue(Arrays.asList("{{" + nameField + "}}1", "{{" + nameField + "}}2"));

        assertThat(doc.removeFromList(path,value)).isFalse();

        doc.appendList(path, value);

        List<String> valueAsList = (List<String>) value.execute(doc.getSource());

        for (String item : valueAsList) {
            assertThat(((List) doc.getField(path)).contains(item)).isTrue();
        }

        assertThat(doc.removeFromList(path, value)).isTrue();

        for (String item : valueAsList) {
            assertThat(((List) doc.getField(path)).contains(item)).isFalse();
        }
    }

    @Test
    public void testAppendAndRemoveFromListWithEmptyTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";
        Doc doc = createDoc(nameField, "Naor",
                dayOfWeekField, "Shabat",
                someField, 5);

        Template path = TemplateService.compileTemplate("list");
        TemplatedValue value = TemplateService.compileValue(Arrays.asList("Naor1", "Naor2"));

        assertThat(doc.removeFromList(path,value)).isFalse();

        doc.appendList(path, value);

        List<String> valueAsList = (List<String>) value.execute(doc.getSource());

        for (String item : valueAsList) {
            assertThat(((List) doc.getField(path)).contains(item)).isTrue();
        }

        assertThat(doc.removeFromList(path, value)).isTrue();

        for (String item : valueAsList) {
            assertThat(((List) doc.getField(path)).contains(item)).isFalse();
        }
    }

    @Test
    public void testHasField() {
        Doc doc = createDoc("int", 15, "String", "test");

        assertThat(doc.hasField("int")).isTrue();
        assertThat(doc.hasField("notExists")).isFalse();
        assertThat(doc.hasField("int", Integer.class)).isTrue();
        assertThat(doc.hasField("int", String.class)).isFalse();
        assertThat(doc.hasField("String", String.class)).isTrue();
        assertThat(doc.hasField("String", Integer.class)).isFalse();
    }
}
