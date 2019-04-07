package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class DeDotProcessorTest {

    private final String messageExample = "{\n" +
            "  \"inner.object\": {\n" +
            "    \"about.us\": \"value\",\n" +
            "    \"registered\": \"Tuesday, March 8, 2016 2:35 PM\",\n" +
            "    \"gps.latitude\": \"-83.019706\",\n" +
            "    \"longitude\": \"6.023174\",\n" +
            "    \"friends\": [{\n" +
            "        \"id\": 0,\n" +
            "        \"first.name\": \"Sheena James\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"id\": 1,\n" +
            "        \"first.name\": \"Castaneda Hinton\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"id\": 2,\n" +
            "        \"first.name\": \"Della Curry\"\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"_id\": \"5c9ce370543222f1cf6b53e0\",\n" +
            "  \"first.name\": {\n" +
            "    \"first\": \"Magdalena\",\n" +
            "    \"last\": \"Stewart\"\n" +
            "  }\n" +
            "}";

    @Test
    public void testDefaultSeperator() throws InterruptedException {

        Map<String, Object> config = new HashMap<>();
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));
        DeDotProcessor deDotProcessor = createProcessor(DeDotProcessor.class, config);
        assertThat(deDotProcessor.process(doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().get("inner.object")).isNull();
        assertThat(doc.getSource().get("first.name")).isNull();

        String seperator =  "_";

        assertThat(doc.getSource().get("first"+ seperator +"name")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner"+ seperator +"object")).get("gps"+ seperator +"latitude")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner"+ seperator +"object")).get("about"+ seperator +"us")).isNotNull();
        ((List<Map<String,Object>>)((Map<String, Object>)
                doc.getSource().get("inner" + seperator + "object")).get("friends")).stream().forEach(
                        friend -> assertThat(friend.get("first"+ seperator +"name")).isNotNull());
    }
    @Test
    public void testCustomSeperator() throws InterruptedException {

        Map<String, Object> config = createConfig("separator","*");
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));

        DeDotProcessor deDotProcessor = createProcessor(DeDotProcessor.class, config);
        assertThat(deDotProcessor.process(doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().get("inner.object")).isNull();
        assertThat(doc.getSource().get("first.name")).isNull();

        String seperator = "*";

        assertThat(doc.getSource().get("first"+ seperator +"name")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner"+ seperator +"object")).get("gps"+ seperator +"latitude")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner"+ seperator +"object")).get("about"+ seperator +"us")).isNotNull();
        ((List<Map<String,Object>>)((Map<String, Object>)
                doc.getSource().get("inner" + seperator + "object")).get("friends")).stream().forEach(
                friend -> assertThat(friend.get("first"+ seperator +"name")).isNotNull());
    }
}
