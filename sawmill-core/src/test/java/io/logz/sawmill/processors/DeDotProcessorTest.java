package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class DeDotProcessorTest {

    String messageExample = "{\n" +
            "  \"inner.object\": {\n" +
            "    \"_id\": \"5c9ce370f7edc93603cfcc5a\",\n" +
            "    \"index\": 3,\n" +
            "    \"guid\": \"6028894d-1aca-4c25-9b65-bb9d496d3b93\",\n" +
            "    \"isActive\": false,\n" +
            "    \"balance\": \"$2,333.93\",\n" +
            "    \"picture\": \"http://placehold.it/32x32\",\n" +
            "    \"age\": 36,\n" +
            "    \"eyeColor\": \"blue\",\n" +
            "    \"name\": {\n" +
            "      \"first\": \"Robin\",\n" +
            "      \"last\": \"Mckay\"\n" +
            "    },\n" +
            "    \"company\": \"FITCORE\",\n" +
            "    \"email\": \"robin.mckay@fitcore.ca\",\n" +
            "    \"phone\": \"+1 (998) 522-3511\",\n" +
            "    \"address\": \"536 Bennet Court, Mappsville, District Of Columbia, 5733\",\n" +
            "    \"about.us\": \"Ut id sint sint proident laboris exercitation cupidatat deserunt aliquip eu cillum officia. Culpa sit ex irure officia eu qui. Non excepteur aliqua voluptate sint magna esse elit consequat mollit mollit reprehenderit dolor. Nisi sit laboris ullamco culpa ullamco laborum commodo fugiat commodo eu aliqua Lorem eiusmod amet. Voluptate sunt consequat mollit quis anim fugiat cillum mollit dolor pariatur. Ipsum ut esse elit tempor. Do ea amet eiusmod laboris tempor deserunt.\",\n" +
            "    \"registered\": \"Tuesday, March 8, 2016 2:35 PM\",\n" +
            "    \"gps.latitude\": \"-83.019706\",\n" +
            "    \"longitude\": \"6.023174\",\n" +
            "    \"tags\": [\n" +
            "      \"mollit\",\n" +
            "      \"qui\",\n" +
            "      \"anim\",\n" +
            "      \"pariatur\",\n" +
            "      \"pariatur\"\n" +
            "    ],\n" +
            "    \"range\": [\n" +
            "      0,\n" +
            "      1,\n" +
            "      2,\n" +
            "      3,\n" +
            "      4,\n" +
            "      5,\n" +
            "      6,\n" +
            "      7,\n" +
            "      8,\n" +
            "      9\n" +
            "    ],\n" +
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
            "    ],\n" +
            "    \"greeting\": \"Hello, Robin! You have 10 unread messages.\",\n" +
            "    \"favoriteFruit\": \"banana\"\n" +
            "  },\n" +
            "  \"_id\": \"5c9ce370543222f1cf6b53e0\",\n" +
            "  \"index\": 0,\n" +
            "  \"guid\": \"d055c342-4c62-4af3-944d-541a835f79a4\",\n" +
            "  \"isActive\": false,\n" +
            "  \"balance\": \"$3,331.30\",\n" +
            "  \"picture\": \"http://placehold.it/32x32\",\n" +
            "  \"age\": 29,\n" +
            "  \"eyeColor\": \"blue\",\n" +
            "  \"first.name\": {\n" +
            "    \"first\": \"Magdalena\",\n" +
            "    \"last\": \"Stewart\"\n" +
            "  }\n" +
            "}";

    @Test
    public void testDefaultSeperator() {

        //Map<String, Object> config = createConfig("seperator","-");
        AssertOnSeperator(messageExample, null);
    }
    @Test
    public void testCustomSeperator() {

        Map<String, Object> config = createConfig("seperator","_");
        AssertOnSeperator(messageExample, config);

        config = createConfig("seperator","#");
        AssertOnSeperator(messageExample, config);
    }


    private void AssertOnSeperator(String messageExample, Map<String, Object> config) {
        Doc doc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));

        Object obk = doc.getSource().get("inner.object");

        assertThat(doc.getSource().get("inner.object")).isNotNull();
        assertThat(doc.getSource().get("first.name")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner.object")).get("gps.latitude")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner.object")).get("about.us")).isNotNull();

        DeDotProcessor deDotProcessor = createProcessor(DeDotProcessor.class, config);

        assertThat(deDotProcessor.process(doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().get("inner.object")).isNull();
        assertThat(doc.getSource().get("first.name")).isNull();

        String seperator = config!=null ? (String)config.get("seperator") : DeDotProcessor.Configuration.DEDOT_DEFAULT_VAL;

        assertThat(doc.getSource().get("first"+ seperator +"name")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner"+ seperator +"object")).get("gps"+ seperator +"latitude")).isNotNull();
        assertThat(((Map<String,Object>) doc.getSource().get("inner"+ seperator +"object")).get("about"+ seperator +"us")).isNotNull();
        assertThat(doc.getSource().get("inner"+ seperator +"object")).isNotNull();
    }
}
