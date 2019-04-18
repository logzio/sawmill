package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static java.net.URLDecoder.decode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UrlDecodeProcessorTest {

    private final String messageExample = "{\n" +
            "  \"innerObject\": {\n" +
            "    \"url\": \"https://github.com/logzio/sawmill/pulls?utf8=%E2%9C%93&q=is%3Apr+is%3Aclosed\",\n" +
            "    \"anotherUrl\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\",\n" +
            "    \"friends\": [{\n" +
            "        \"id\": \"%%1%20%3A\",\n" +
            "        \"numOfFriends\": 55,\n" +
            "        \"hasLaptop\": true,\n" +
            "        \"url\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"id\": \"%%1%20%3A\",\n" +
            "        \"numOfFriends\": 56,\n" +
            "        \"hasLaptop\": false,\n" +
            "        \"url\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"id\": \"%%1%20%3A\",\n" +
            "        \"numOfFriends\": 20,\n" +
            "        \"hasLaptop\": true,\n" +
            "        \"url\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\"\n" +
            "      }\n]," +
            "    \"Indicators\": [true,true,false]\n," +
            "    \"grades\": [89,88,100]\n," +
            "    \"shadows\": [\"shadow1\",\"shadow2\",\"shadow3\"]\n," +
            "    \"urls\": [\"https://github.com/logzio/sawmill/pulls?utf8=%E2%9C%93&q=is%3Apr+is%3Aclosed\",\"https://github.com/logzio/sawmill/pulls?utf8=%E2%9C%93&q=is%3Apr+is%3Aclosed\"]\n" +
            "  },\n" +
            "\"url\": \"https://github.com/logzio/sawmill/pulls?utf8=%E2%9C%93&q=is%3Apr+is%3Aclosed\",\n" +
            "\"id\": \"1%20%3A\"\n" +
            "}";


    @Test
    public void testAllFieldsAndCodec() throws InterruptedException, UnsupportedEncodingException {
        String encoding = "ibm856";

        Map<String, Object> config = createConfig("allFields","true","charset",encoding);

        UrlDecodeProcessor urlDecodeProcessor = createProcessor(UrlDecodeProcessor.class, config);

        Doc processedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));

        assertThat(urlDecodeProcessor.process(processedDoc).isSucceeded()).isTrue();

        Doc unprocessedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));

        assertThat((String) processedDoc.getField("url")).isEqualTo(decode(unprocessedDoc.getField("url"), encoding));
        assertThat((String) processedDoc.getField("id")).isEqualTo(decode(unprocessedDoc.getField("id"),encoding));
        assertThat((String) processedDoc.getField("innerObject.url")).isEqualTo(decode(unprocessedDoc.getField("innerObject.url"),encoding));
        assertThat((String) processedDoc.getField("innerObject.anotherUrl")).isEqualTo(decode(unprocessedDoc.getField("innerObject.anotherUrl"),encoding));

        List<Map<String,Object>> processedfriends = ((List)processedDoc.getField("innerObject.friends"));
        List<Map<String,Object>> unProcessedfriends = ((List)unprocessedDoc.getField("innerObject.friends"));

        for(int i = 0;i<processedfriends.size();++i){
            assertThat(processedfriends.get(i).get("url")).isEqualTo(decode((String) unProcessedfriends.get(i).get("url"),encoding));
            assertThat(processedfriends.get(i).get("id")).isEqualTo((String)unProcessedfriends.get(i).get("id"));
            assertThat(processedfriends.get(i).get("numOfFriends")).isEqualTo(unProcessedfriends.get(i).get("numOfFriends"));
            assertThat(processedfriends.get(i).get("hasLaptop")).isEqualTo(unProcessedfriends.get(i).get("hasLaptop"));
        }
        List<Boolean> processedIndicators = processedDoc.getField("innerObject.Indicators");
        List<Boolean> unProcessedIndicators = unprocessedDoc.getField("innerObject.Indicators");

        for(int i =0;i<processedIndicators.size();i++){
            assertThat(processedIndicators.get(i).equals(unProcessedIndicators.get(i)));
        }

        List<Integer> processedGrades = processedDoc.getField("innerObject.grades");
        List<Integer> unProcessedGrades = unprocessedDoc.getField("innerObject.grades");

        for(int i =0;i<processedGrades.size();i++){
            assertThat(processedGrades.get(i).equals(unProcessedGrades.get(i)));
        }

        List<String> processedShadows = processedDoc.getField("innerObject.shadows");
        List<String> unProcessedShadows = unprocessedDoc.getField("innerObject.shadows");

        for(int i =0;i<processedShadows.size();i++){
            assertThat(processedShadows.get(i).equals(unProcessedShadows.get(i)));
        }

        List<String> processedUrls = processedDoc.getField("innerObject.urls");
        List<String> unProcessedUrls = unprocessedDoc.getField("innerObject.urls");

        for(int i =0;i<processedUrls.size();i++){
           assertThat(processedUrls.get(i).equals(decode(unProcessedUrls.get(i),encoding)));
        }
    }

    @Test
    public void testSingleFieldAndDefaultEncoding() throws InterruptedException, UnsupportedEncodingException {
        String encoding = "UTF-8";
        Map<String, Object> config = createConfig("field","innerObject.url");
        UrlDecodeProcessor urlDecodeProcessor = createProcessor(UrlDecodeProcessor.class, config);
        Doc processedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));
        assertThat(urlDecodeProcessor.process(processedDoc).isSucceeded()).isTrue();
        Doc unprocessedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));
        assertThat((String) processedDoc.getField("innerObject.url")).isEqualTo(decode(unprocessedDoc.getField("innerObject.url"),encoding));
    }

    @Test
    public void testNoneExistingField() throws InterruptedException {
        Map<String, Object> config = createConfig("field","noneExisting.field");
        UrlDecodeProcessor urlDecodeProcessor = createProcessor(UrlDecodeProcessor.class, config);
        Doc processedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));
        assertThat(urlDecodeProcessor.process(processedDoc).isSucceeded()).isFalse();

        assertThat(((List) processedDoc.getField("tags")).get(0).equals("_urldecodefailure"));
    }

    @Test
    public void testBadConfigField() throws InterruptedException {
        Map<String, Object> config = createConfig("charset","noneExistingCharset");
        assertThatThrownBy(() -> createProcessor(UrlDecodeProcessor.class, config)).isInstanceOf(ProcessorConfigurationException.class);
    }
}
