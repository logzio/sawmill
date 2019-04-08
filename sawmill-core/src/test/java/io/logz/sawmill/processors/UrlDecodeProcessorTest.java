package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.net.URLDecoder;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class UrlDecodeProcessorTest {

    private final String messageExample = "{\n" +
            "  \"innerObject\": {\n" +
            "    \"url\": \"https://github.com/logzio/sawmill/pulls?utf8=%E2%9C%93&q=is%3Apr+is%3Aclosed\",\n" +
            "    \"anotherUrl\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\",\n" +
            "    \"friends\": [{\n" +
            "        \"id\": \"1%20%3A\",\n" +
            "        \"url\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"id\": \"%%1%20%3A\",\n" +
            "        \"url\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"id\": \"1%20%3A\",\n" +
            "        \"url\": \"https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Durl%2Bencode%2Bexample%26oq%3Durl%2Bencode%2Bexample%26aqs%3Dchrome.0.0l6.2790j0j4%26sourceid%3Dchrome%26ie%3DUTF-8\"\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "\"url\": \"https://github.com/logzio/sawmill/pulls?utf8=%E2%9C%93&q=is%3Apr+is%3Aclosed\",\n" +
            "\"id\": \"1%20%3A\"\n" +
            "}";


    @Test
    public void testAllFieldsAndCodec() throws InterruptedException {
        String encoding = "ibm856";

        Map<String, Object> config = createConfig("allFields","true","charSet",encoding);

        UrlDecodeProcessor urlDecodeProcessor = createProcessor(UrlDecodeProcessor.class, config);

        Doc processedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));

        assertThat(urlDecodeProcessor.process(processedDoc).isSucceeded()).isTrue();

        Doc unprocessedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));

        assertThat((String) processedDoc.getField("url")).isEqualTo(decodeUrl(unprocessedDoc.getField("url"), encoding));
        assertThat((String) processedDoc.getField("id")).isEqualTo(decodeUrl(unprocessedDoc.getField("id"),encoding));
        assertThat((String) processedDoc.getField("innerObject.url")).isEqualTo(decodeUrl(unprocessedDoc.getField("innerObject.url"),encoding));
        assertThat((String) processedDoc.getField("innerObject.anotherUrl")).isEqualTo(decodeUrl(unprocessedDoc.getField("innerObject.anotherUrl"),encoding));

        List<Map<String,Object>> processedfriends = ((List)processedDoc.getField("innerObject.friends"));
        List<Map<String,Object>> unProcessedfriends = ((List)unprocessedDoc.getField("innerObject.friends"));

        for(int i = 0;i<processedfriends.size();++i){
            assertThat(processedfriends.get(i).get("url")).isEqualTo(decodeUrl((String) unProcessedfriends.get(i).get("url"),encoding));
            assertThat(processedfriends.get(i).get("id")).isEqualTo(decodeUrl((String)unProcessedfriends.get(i).get("id"),encoding));
        }
    }
    @Test
    public void testSingleFieldAndDefaultEncoding() throws InterruptedException {
        String encoding = "UTF-8";
        Map<String, Object> config = createConfig("field","innerObject.url");
        UrlDecodeProcessor urlDecodeProcessor = createProcessor(UrlDecodeProcessor.class, config);
        Doc processedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));
        assertThat(urlDecodeProcessor.process(processedDoc).isSucceeded()).isTrue();
        Doc unprocessedDoc = new Doc(JsonUtils.fromJsonString(Map.class,messageExample));
        assertThat((String) processedDoc.getField("innerObject.url")).isEqualTo(decodeUrl(unprocessedDoc.getField("innerObject.url"),encoding));
    }

    private String decodeUrl(String valueToDecode,String charSet) {
        String decodedUrl = valueToDecode;
        try{
            decodedUrl = URLDecoder.decode(valueToDecode,charSet);
        }
        catch (Exception ignored){}
        return decodedUrl;
    }
}
