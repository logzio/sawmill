package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AnonymizeProcessorTest {
    public String key = "thisIsKey";

    @Test
    public void testBadConfig() {
        assertThatThrownBy(() -> createProcessor(AnonymizeProcessor.class, createConfig("key", key))).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(AnonymizeProcessor.class, createConfig("fields", Arrays.asList("1")))).isInstanceOf(ProcessorConfigurationException.class);
    }

    @Test
    public void testSHA1() {
        Map<String, Object> config = createConfig("fields", Arrays.asList("field1", "field2", "nonExistsField"),
                "key", key);

        Doc doc = createDoc("field1", "value1",
                "field2", "value2");

        AnonymizeProcessor anonymizeProcessor = createProcessor(AnonymizeProcessor.class, config);

        ProcessResult processResult = anonymizeProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isFalse();
        String errorMessage = processResult.getError().get().getMessage();
        assertThat((String)doc.getField("field1")).isEqualTo(DigestUtils.sha1Hex("value1"));
        assertThat((String)doc.getField("field2")).isEqualTo(DigestUtils.sha1Hex("value2"));
        assertThat(errorMessage.contains("nonExistsField")).isTrue();
    }

    @Test
    public void testSHA256() {
        Map<String, Object> config = createConfig("fields", Arrays.asList("field1", "field2"),
                "key", key,
                "algorithm", "SHA256");

        Doc doc = createDoc("field1", "value1",
                "field2", "value2");

        AnonymizeProcessor anonymizeProcessor = createProcessor(AnonymizeProcessor.class, config);

        ProcessResult processResult = anonymizeProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("field1")).isEqualTo(DigestUtils.sha256Hex("value1"));
        assertThat((String)doc.getField("field2")).isEqualTo(DigestUtils.sha256Hex("value2"));
    }

    @Test
    public void testSHA384() {
        Map<String, Object> config = createConfig("fields", Arrays.asList("field1", "field2"),
                "key", key,
                "algorithm", "SHA384");

        Doc doc = createDoc("field1", "value1",
                "field2", "value2");

        AnonymizeProcessor anonymizeProcessor = createProcessor(AnonymizeProcessor.class, config);

        ProcessResult processResult = anonymizeProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("field1")).isEqualTo(DigestUtils.sha384Hex("value1"));
        assertThat((String)doc.getField("field2")).isEqualTo(DigestUtils.sha384Hex("value2"));
    }

    @Test
    public void testSHA512() {
        Map<String, Object> config = createConfig("fields", Arrays.asList("field1", "field2"),
                "key", key,
                "algorithm", "SHA512");

        Doc doc = createDoc("field1", "value1",
                "field2", "value2");

        AnonymizeProcessor anonymizeProcessor = createProcessor(AnonymizeProcessor.class, config);

        ProcessResult processResult = anonymizeProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("field1")).isEqualTo(DigestUtils.sha512Hex("value1"));
        assertThat((String)doc.getField("field2")).isEqualTo(DigestUtils.sha512Hex("value2"));
    }

    @Test
    public void testMD5() {
        Map<String, Object> config = createConfig("fields", Arrays.asList("field1", "field2"),
                "key", key,
                "algorithm", "MD5");

        Doc doc = createDoc("field1", "value1",
                "field2", "value2");

        AnonymizeProcessor anonymizeProcessor = createProcessor(AnonymizeProcessor.class, config);

        ProcessResult processResult = anonymizeProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String)doc.getField("field1")).isEqualTo(DigestUtils.md5Hex("value1"));
        assertThat((String)doc.getField("field2")).isEqualTo(DigestUtils.md5Hex("value2"));
    }
}
