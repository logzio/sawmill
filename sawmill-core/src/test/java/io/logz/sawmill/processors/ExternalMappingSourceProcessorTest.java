package io.logz.sawmill.processors;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.utils.FactoryUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
    public static final String NOT_FOUND_MAPPING = "/404";

    private static WireMockServer wireMockServer;
    private static Integer port;

    @BeforeClass
    public static void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        createStubs();
        wireMockServer.start();
        port = wireMockServer.port();
    }

    private static void createStubs() {
        wireMockServer.stubFor(get(BOOKS_MAPPING)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "Charles Dickens = Oliver Twist, A Christmas Carol, The Chimes\n" +
                            "Jack London = White Fang, Martin Eden, Hearts of Three\n" +
                            "Ernest Hemingway = For Whom the Bell Tolls, A Farewell to Arms, The Old Man and the Sea\n"
                    )
            )
        );
        wireMockServer.stubFor(get(EMPTY_MAPPING).willReturn(aResponse().withBody("").withStatus(200)));
        wireMockServer.stubFor(get(ILLEGAL_FORMAT_MAPPING).willReturn(aResponse().withStatus(200).withBody("a, b, c")));
        wireMockServer.stubFor(get(NOT_FOUND_MAPPING).willReturn(aResponse().withStatus(404).withBody("Not Found")));
    }

    @AfterClass
    public static void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testExternalMappingSourceProcessor() throws InterruptedException {
        ExternalMappingSourceProcessor processor = createProcessor(BOOKS_MAPPING);

        Doc firstDoc = createDoc(SOURCE_FIELD_NAME, "Charles Dickens");
        processor.process(firstDoc);

        assertThat(firstDoc.hasField(TARGET_FIELD_NAME)).isTrue();
        List<String> targetField1 = firstDoc.getField(TARGET_FIELD_NAME);
        assertThat(targetField1).containsAll(Arrays.asList("Oliver Twist", "A Christmas Carol", "The Chimes"));

        Doc secondDoc = createDoc(SOURCE_FIELD_NAME, "Ernest Hemingway");
        processor.process(secondDoc);

        assertThat(secondDoc.hasField(TARGET_FIELD_NAME)).isTrue();
        List<String> targetField2 = secondDoc.getField(TARGET_FIELD_NAME);
        assertThat(targetField2).containsAll(Arrays.asList("For Whom the Bell Tolls", "A Farewell to Arms", "The Old Man and the Sea"));
    }

    @Test
    public void testExternalMappingSourceProcessorMissingMapping() throws InterruptedException {
        ExternalMappingSourceProcessor processor = createProcessor(BOOKS_MAPPING);

        Doc doc = createDoc(SOURCE_FIELD_NAME, "Unknown Author");
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD_NAME)).isTrue();
        List<String> targetField = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetField).isEmpty();
    }

    @Test
    public void testMappingIsEmpty() throws InterruptedException {
        ExternalMappingSourceProcessor processor = createProcessor(EMPTY_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "test");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testNotFoundMapping() throws InterruptedException {
        ExternalMappingSourceProcessor processor = createProcessor(NOT_FOUND_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "test");
        processor.process(doc);

        assertContainsExternalMappingProcessorFailureTag(doc);
    }

    @Test
    public void testIllegalFormat() throws InterruptedException {
        ExternalMappingSourceProcessor processor = createProcessor(ILLEGAL_FORMAT_MAPPING);
        Doc doc = createDoc(SOURCE_FIELD_NAME, "test");
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
            "mappingRefreshPeriodInSeconds", 1
        );

        assertThatThrownBy(() -> FactoryUtils.createProcessor(ExternalMappingSourceProcessor.class, fourthConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mappingRefreshPeriodInSeconds");
    }

    private void assertContainsExternalMappingProcessorFailureTag(Doc doc) {
        assertThat(doc.hasField(TARGET_FIELD_NAME)).isFalse();
        assertThat(doc.hasField("tags")).isTrue();
        List<String> tags = doc.getField("tags");
        assertThat(tags).contains("_externalSourceMappingFailure");
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
