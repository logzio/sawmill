package io.logz.sawmill;

import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class DocTest {

    @Test
    public void testGetField() {
        Doc doc = createDoc("message", "holla",
                "object", JsonUtils.createMap("nestedField", "nestedValue", "field.with.dots", "hola"),
                "endsWithEscapeChar\\", JsonUtils.createMap("nestedField", "nestedValue2")
        );

        assertThat((String) doc.getField("message")).isEqualTo("holla");
        assertThat((String) doc.getField("object.nestedField")).isEqualTo("nestedValue");
        assertThat((String) doc.getField("object.field\\.with\\.dots")).isEqualTo("hola");
        assertThat((String) doc.getField("endsWithEscapeChar\\\\.nestedField")).isEqualTo("nestedValue2");
        assertThatThrownBy(() -> doc.getField("notExists")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testHasField() {
        Doc doc = createDoc("int", 15, "String", "test", "object",
                JsonUtils.createMap("nestedField", "nestedValue")
        );

        assertThat(doc.hasField("int")).isTrue();
        assertThat(doc.hasField("notExists")).isFalse();
        assertThat(doc.hasField("notExists", String.class)).isFalse();
        assertThat(doc.hasField("int", Integer.class)).isTrue();
        assertThat(doc.hasField("int", String.class)).isFalse();
        assertThat(doc.hasField("String", String.class)).isTrue();
        assertThat(doc.hasField("String", Integer.class)).isFalse();
        assertThat(doc.hasField("object.nestedField", String.class)).isTrue();

    }

    @Test
    public void testAddField() {
        Doc doc = createDoc("message", "hola", "name", "test");

        doc.addField("message" ,"hola2");
        assertThat((String) doc.getField("message")).isEqualTo("hola2");

        doc.addField("message.nestedMessage", 15);
        assertThat((Integer) doc.getField("message.nestedMessage")).isEqualTo(15);

        doc.addField("int" ,15);
        assertThat((Integer) doc.getField("int")).isEqualTo(15);

        doc.addField("object.nested\\Field1", "shalom1");
        assertThat((String) doc.getField("object.nested\\Field1")).isEqualTo("shalom1");

        doc.addField("object.nestedField2", "shalom2");
        assertThat((String) doc.getField("object.nestedField2")).isEqualTo("shalom2");

        doc.addField("object.field\\.with\\.dots", "shalom3");
        assertThat(((Map<String, Object>) doc.getSource().get("object")).get("field.with.dots")).isEqualTo("shalom3");
    }

    @Test
    public void testRemoveField() {
        Doc doc = createDoc("message", "hola", "name", "test", "object",
                JsonUtils.createMap("nestedField1", "nestedValue1", "nestedField2", "nestedValue2",
                        "fieldWith.escaping", "someValue", ".startWithEscaping", "value2")
        );

        assertThat(doc.removeField("message")).isTrue();
        assertThat(doc.hasField("message")).isFalse();
        assertThat(doc.removeField("message")).isFalse();

        assertThat(doc.removeField("object.nestedField2")).isTrue();
        assertThat(doc.hasField("object.nestedField2")).isFalse();

        assertThat(doc.removeField("object.fieldWith\\.escaping")).isTrue();
        assertThat(doc.hasField("object.fieldWith\\.escaping")).isFalse();

        assertThat(doc.removeField("object.\\.startWithEscaping")).isTrue();
        assertThat(doc.hasField("object.\\.startWithEscaping")).isFalse();
    }

    @Test
    public void testAppendList() {
        Doc doc = createDoc("message", "hola", "name", "test");

        doc.appendList("message", "hola2");
        assertThat(((List) doc.getField("message"))).isEqualTo(Arrays.asList("hola", "hola2"));

        doc.appendList("list", "value1");
        assertThat(((List) doc.getField("list"))).isEqualTo(Arrays.asList("value1"));

        doc.appendList("list", Arrays.asList("value2", "value3"));
        assertThat(((List) doc.getField("list"))).isEqualTo(Arrays.asList("value1", "value2", "value3"));
    }

    @Test
    public void testRemoveFromList() {
        Doc doc = createDoc("message", "hola", "name", "test");

        assertThat(doc.removeFromList("message", "hola")).isFalse();
        assertThat(doc.removeFromList("notExists", "whatever")).isFalse();

        doc.appendList("list", Arrays.asList("value1", "value2"));

        assertThat(doc.removeFromList("list", "value2")).isTrue();
        assertThat(((List) doc.getField("list"))).isEqualTo(Arrays.asList("value1"));

        assertThat(doc.removeFromList("list", Arrays.asList("value1"))).isTrue();
    }
}
