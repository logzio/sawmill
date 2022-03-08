package io.logz.sawmill.processors;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import io.logz.sawmill.Doc;
import io.logz.sawmill.utils.FactoryUtils;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExternalMappingSourceProcessorTest {

    public static final String SOURCE_FIELD_NAME = "author";
    public static final String TARGET_FIELD_NAME = "books";

    public static final String BOOKS_MAPPING = "/books";
    public static final String EMPTY_MAPPING = "/empty";
    public static final String ILLEGAL_FORMAT_MAPPING = "/illegalFormatMapping";
    public static final String LARGE_FILE_MAPPING = "/largeFile";
    public static final String NOT_FOUND_MAPPING = "/404";
    public static final String EMPTY_KEY_MAPPING = "/emptyKey";
    public static final String EMPTY_VALUE_MAPPING = "/emptyValue";

    private static Integer port;

    @ClassRule
    public static WireMockClassRule wireMockRule = new WireMockClassRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Rule
    public WireMockClassRule instanceRule = wireMockRule;

    @BeforeClass
    public static void setUp() {
        port = wireMockRule.port();
    }

    @Test
    public void testExternalMappingSourceProcessor() throws InterruptedException {
        setUpBooksMappingStub();
        ExternalMappingSourceProcessor processor = createProcessor(BOOKS_MAPPING);

        /* first author */
        Doc firstDoc = createDoc(SOURCE_FIELD_NAME, "Charles Dickens");
        processor.process(firstDoc);

        assertThat(firstDoc.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField1 = firstDoc.getField(TARGET_FIELD_NAME);
        assertThat(targetField1).containsAll(Arrays.asList("Oliver Twist", "A Christmas Carol", "The Chimes"));

        /* second author - key contains special characters */
        Doc secondDoc = createDoc(SOURCE_FIELD_NAME, "\"Ernest Hemingway\"");
        processor.process(secondDoc);

        assertThat(secondDoc.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField2 = secondDoc.getField(TARGET_FIELD_NAME);
        assertThat(targetField2).containsAll(Arrays.asList("For Whom the Bell Tolls", "A Farewell to Arms", "The Old Man and the Sea"));

        /* third author - the same key occurring multiple times in the mapping */
        Doc thirdDoc = createDoc(SOURCE_FIELD_NAME, "Jack London");
        processor.process(thirdDoc);

        assertThat(thirdDoc.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField3 = thirdDoc.getField(TARGET_FIELD_NAME);
        assertThat(targetField3).containsAll(Arrays.asList("White Fang", "Martin Eden", "The Sea Wolf"));
    }

    @Test
    public void testExternalMappingSourceProcessorMissingMapping() throws InterruptedException {
        setUpBooksMappingStub();
        ExternalMappingSourceProcessor processor = createProcessor(BOOKS_MAPPING);

        Doc doc = createDoc(SOURCE_FIELD_NAME, "Unknown Author");
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetField).isEmpty();
    }

    @Test
    public void testEmptyMapping() throws InterruptedException {
        wireMockRule.stubFor(get(EMPTY_MAPPING).willReturn(aResponse().withBody("").withStatus(200)));

        ExternalMappingSourceProcessor processor = createProcessor(EMPTY_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "test");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testEmptyKey() throws InterruptedException {
        wireMockRule.stubFor(get(EMPTY_KEY_MAPPING).willReturn(aResponse().withBody(" = b").withStatus(200)));

        ExternalMappingSourceProcessor processor = createProcessor(EMPTY_KEY_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testEmptyValue() throws InterruptedException {
        wireMockRule.stubFor(get(EMPTY_VALUE_MAPPING).willReturn(aResponse().withBody("a = ").withStatus(200)));

        ExternalMappingSourceProcessor processor = createProcessor(EMPTY_VALUE_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "a");
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetField).hasSize(1);
        assertThat(targetField).contains("");
    }

    @Test
    public void testNotFoundMapping() throws InterruptedException {
        wireMockRule.stubFor(get(NOT_FOUND_MAPPING).willReturn(aResponse().withStatus(404).withBody("Not Found")));

        ExternalMappingSourceProcessor processor = createProcessor(NOT_FOUND_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "test");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testIllegalFormat() throws InterruptedException {
        wireMockRule.stubFor(get(ILLEGAL_FORMAT_MAPPING).willReturn(aResponse().withStatus(200).withBody("a, b, c")));

        ExternalMappingSourceProcessor processor = createProcessor(ILLEGAL_FORMAT_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "test");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testMappingFileExceedsMaximumSize() throws InterruptedException {
        wireMockRule.stubFor(get(LARGE_FILE_MAPPING).willReturn(
            aResponse().withStatus(200).withHeader(HttpHeaders.CONTENT_LENGTH, "60000000").withBody("a = b, c"))
        );

        ExternalMappingSourceProcessor processor = createProcessor(LARGE_FILE_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "test");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testMappingFileExceedsMaximumLength() throws InterruptedException {
        wireMockRule.stubFor(get(LARGE_FILE_MAPPING).willReturn(
            aResponse().withStatus(200)
                .withHeader(HttpHeaders.CONTENT_LENGTH, "5000000")
                .withBody(StringUtils.repeat("a=b\n", (int) ExternalMappingSourceProcessor.Constants.EXTERNAL_MAPPING_MAX_LINES + 1))
            )
        );

        ExternalMappingSourceProcessor processor = createProcessor(LARGE_FILE_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "a");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testMissingConfigurationsFailsCreatingProcessor() {
        Map<String, Object> firstConfig = ImmutableMap.of(
            "targetField", TARGET_FIELD_NAME,
            "mappingSourceUrl", "http://localhost:" + port + BOOKS_MAPPING
        );

        assertThatThrownBy(() -> FactoryUtils.createProcessor(ExternalMappingSourceProcessor.class, firstConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("sourceField");

        Map<String, Object> secondConfig = ImmutableMap.of(
            "sourceField", SOURCE_FIELD_NAME,
            "mappingSourceUrl", "http://localhost:" + port + BOOKS_MAPPING
        );

        assertThatThrownBy(() -> FactoryUtils.createProcessor(ExternalMappingSourceProcessor.class, secondConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("targetField");

        Map<String, Object> thirdConfig = ImmutableMap.of(
            "sourceField", SOURCE_FIELD_NAME,
            "targetField", TARGET_FIELD_NAME
        );

        assertThatThrownBy(() -> FactoryUtils.createProcessor(ExternalMappingSourceProcessor.class, thirdConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mappingSourceUrl");

        Map<String, Object> fourthConfig = ImmutableMap.of(
            "sourceField", SOURCE_FIELD_NAME,
            "targetField", TARGET_FIELD_NAME,
            "mappingSourceUrl", "http://localhost:" + port + BOOKS_MAPPING,
            "mappingRefreshPeriodInMillis", 1
        );

        assertThatThrownBy(() -> FactoryUtils.createProcessor(ExternalMappingSourceProcessor.class, fourthConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mappingRefreshPeriodInMillis");
    }

    private void setUpBooksMappingStub() {
        wireMockRule.stubFor(get(BOOKS_MAPPING)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain; charset=utf-8")
                    .withBodyFile("books_mapping.properties")
            )
        );
    }

    private void assertContainsExternalMappingProcessorFailureTag(Doc doc) {
        assertThat(doc.hasField(TARGET_FIELD_NAME)).isFalse();
        assertThat(doc.hasField("tags")).isTrue();
        Iterable<String> tags = doc.getField("tags");
        assertThat(tags).contains(ExternalMappingSourceProcessor.Constants.PROCESSOR_FAILURE_TAG);
    }

    private ExternalMappingSourceProcessor createProcessor(String mappingPath) {
        Map<String, Object> config = ImmutableMap.of(
            "sourceField", SOURCE_FIELD_NAME,
            "targetField", TARGET_FIELD_NAME,
            "mappingSourceUrl", "http://localhost:" + port + mappingPath
        );

        return FactoryUtils.createProcessor(ExternalMappingSourceProcessor.class, config);
    }
}
