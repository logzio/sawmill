package io.logz.sawmill;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        doc.removeField(path);

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> doc.removeField(path)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testAppendAndRemoveFromList() {
        Doc doc = createDoc("message", "hola", "name", "test");

        String path = "list";
        List<String> value = Arrays.asList("value1", "value2");

        assertThatThrownBy(() -> doc.removeFromList(path,value)).isInstanceOf(IllegalStateException.class);

        doc.appendList(path, value);

        for (String item : value) {
            assertThat(((List) doc.getField("list")).contains(item)).isTrue();
        }

        doc.removeFromList(path, value);

        for (String item : value) {
            assertThat(((List) doc.getField("list")).contains(item)).isFalse();
        }
    }
}
