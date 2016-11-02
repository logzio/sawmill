import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertNotNull;

public class DocTest {

    @Test
    public void testAddAndGetFieldValue() {
        Map<String,Object> source = new HashMap<>();
        source.put("message", "hola");
        source.put("name", "test");
        source.put("object", ImmutableMap.of("nestedField", "shalom"));
        String value = "shalom";
        String path = "object.nestedField";

        Doc doc = new Doc(source);

        String fieldName = "newField";
        String fieldValue = "hello";
        doc.addField(fieldName, fieldValue);

        assertNotNull(doc.getSource().get(fieldName));
        assertThat(doc.getSource().get(fieldName)).isEqualTo(fieldValue);
        assertThat((String) doc.getField(path)).isEqualTo(value);
    }
}
