package io.logz.sawmill;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.TemplateFactory.compileTemplate;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class DocTest {

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
    public void testHasField() {
        Doc doc = createDoc("int", 15, "String", "test");

        assertThat(doc.hasField("int")).isTrue();
        assertThat(doc.hasField("notExists")).isFalse();
        assertThat(doc.hasField("int", Integer.class)).isTrue();
        assertThat(doc.hasField("int", String.class)).isFalse();
        assertThat(doc.hasField("String", String.class)).isTrue();
        assertThat(doc.hasField("String", Integer.class)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldPathAndValueTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";

        Doc doc = createDoc(nameField, "Jimmy",
                dayOfWeekField, "Shabat",
                someField, ".nested");

        Template path = compileTemplate("field{{" + someField + "}}").get();
        Template value = compileTemplate("Hey {{" + nameField + "}}, {{" + dayOfWeekField + "}} is a beautiful day").get();

        doc.addField(path, value);

        assertThat(doc.hasField(path)).isTrue();
        assertThat((String) doc.getField(path)).isEqualTo("Hey Jimmy, Shabat is a beautiful day");

        assertThat(doc.removeField(path)).isTrue();
        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAddGetAndRemoveFieldPathTemplate() {
        String nameField = "name";
        String dayOfWeekField = "dayofweek";
        String someField = "someField";

        Doc doc = createDoc(nameField, "Jimmy",
                dayOfWeekField, "Shabat",
                someField, ".nested");

        Template path = compileTemplate("field{{" + someField + "}}").get();
        List value = Arrays.asList("this", "is", "list");

        doc.addField(path, value);

        assertThat(doc.hasField(path)).isTrue();
        assertThat((List) doc.getField(path)).isEqualTo(value);

        assertThat(doc.removeField(path)).isTrue();
        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThat(doc.removeField(path)).isFalse();
    }

    @Test
    public void testAppendAndRemoveFromListPathAndValueTemplate() {
        String nameField = "name";
        String tagsField = "tags";
        String someField = "someField";

        Doc doc = createDoc(nameField, "Jimmy",
                tagsField, "tag1",
                someField, "tag");

        Template path = compileTemplate("{{" + someField + "}}s").get();
        Template value = compileTemplate("tag{{" + nameField + "}}").get();

        assertThat(doc.removeFromList(path,value)).isFalse();

        doc.appendList(path, value);

        assertThat((List) doc.getField(path)).isEqualTo(Arrays.asList("tag1", "tagJimmy"));

        assertThat(doc.removeFromList(path,value)).isTrue();

        assertThat((List) doc.getField(path)).isEqualTo(Arrays.asList("tag1"));
    }

    @Test
    public void testAppendAndRemoveFromListPathTemplate() {
        String nameField = "name";
        String tagsField = "tags";
        String someField = "someField";

        Doc doc = createDoc(nameField, "Jimmy",
                tagsField, "tag1",
                someField, "tag");

        Template path = compileTemplate("{{" + someField + "}}s").get();
        String value = "tagJimmy";

        assertThat(doc.removeFromList(path,value)).isFalse();

        doc.appendList(path, value);

        assertThat((List) doc.getField(path)).isEqualTo(Arrays.asList("tag1", "tagJimmy"));

        assertThat(doc.removeFromList(path,value)).isTrue();

        assertThat((List) doc.getField(path)).isEqualTo(Arrays.asList("tag1"));
    }

    @Test
    public void testAppendAndRemoveFromListValueTemplate() {
        String nameField = "name";
        String tagsField = "tags";
        String someField = "someField";

        Doc doc = createDoc(nameField, "Jimmy",
                tagsField, "tag1",
                someField, "tag");

        String path = tagsField;
        Template value = compileTemplate("tag{{" + nameField + "}}").get();

        assertThat(doc.removeFromList(path,value)).isFalse();

        doc.appendList(path, value);

        assertThat((List) doc.getField(path)).isEqualTo(Arrays.asList("tag1", "tagJimmy"));

        assertThat(doc.removeFromList(path,value)).isTrue();

        assertThat((List) doc.getField(path)).isEqualTo(Arrays.asList("tag1"));
    }
}
