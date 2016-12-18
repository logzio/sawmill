package io.logz.sawmill;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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
}
