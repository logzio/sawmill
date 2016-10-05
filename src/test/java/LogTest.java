import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Log;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

public class LogTest {

    @Test
    public void testConstructionWithInvalidArguments() {
        assertThatThrownBy(() -> new Log(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Log(new HashMap<>())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Log(null, new HashMap<>())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Log(new HashMap<>(), new HashMap<>())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConstructionWithSource() {
        Map<String,Object> source = ImmutableMap.of("message", "hola",
                "type", "test");
        Log log = new Log(source);

        assertThat(log.getSource()).isEqualTo(source);
    }

    @Test
    public void testConstructionWithSourceAndMetadata() {
        Map<String,Object> source = ImmutableMap.of("message", "hola",
                "type", "test");
        Map<String,Object> metadata = ImmutableMap.of("id","1");
        Log log = new Log(source, metadata);

        assertThat(log.getSource()).isEqualTo(source);
        assertThat(log.getMetadata()).isEqualTo(metadata);
    }

    @Test
    public void testGetFieldValue() {
        Map<String,Object> source = ImmutableMap.of("message", "hola",
                "type", "test",
                "object", ImmutableMap.of("nestedField", "shalom"));
        String value = "shalom";
        String path = "object.nestedField";

        Log log = new Log(source);

        assertThat(log.getFieldValue(path, String.class)).isEqualTo(value);
    }

    @Test
    public void testAddFieldValue() {
        Map<String,Object> source = new HashMap<>();
        source.put("message", "hola");
        Log log = new Log(source);

        String fieldName = "newField";
        String fieldValue = "hello";
        log.addFieldValue(fieldName, fieldValue);

        assertNotNull(log.getSource().get(fieldName));
        assertThat(log.getSource().get(fieldName)).isEqualTo(fieldValue);
    }
}
