import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class DocTest {

    @Test
    public void testAddGetAndRemoveFieldValue() {
        Map<String,Object> source = new HashMap<>();
        source.put("message", "hola");
        source.put("name", "test");
        String value = "shalom";
        String path = "object.nestedField";

        Doc doc = new Doc(source);

        doc.addField(path, value);

        assertThat((String) doc.getField(path)).isEqualTo(value);

        doc.removeField(path);

        assertThatThrownBy(() -> doc.getField(path)).isInstanceOf(IllegalStateException.class);
    }
}
