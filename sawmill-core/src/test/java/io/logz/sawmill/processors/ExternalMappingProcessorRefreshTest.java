package io.logz.sawmill.processors;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import io.logz.sawmill.Doc;
import java.net.MalformedURLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ExternalMappingProcessorRefreshTest {

    public static final String SOURCE_FIELD_NAME = "author";
    public static final String TARGET_FIELD_NAME = "books";
    public static final int DISABLE_MAPPING_REFRESH = -1;

    public static final String BOOKS_MAPPING = "/books";

    public static final String MAPPING_REFRESH_SCENARIO = "Books Mapping Refresh";
    public static final String MAPPING_NOT_FOUND_AFTER_REFRESH_SCENARIO = "Books Mapping Not Found After Refresh";
    public static final String MAPPING_NOT_MODIFIED_AFTER_REFRESH_SCENARIO = "Books Mapping Not Modified After Refresh";
    public static final String UPDATED_BOOKS_MAPPING_STATE = "Updated Books Mapping";
    public static final String BOOKS_MAPPING_NOT_MODIFIED_STATE = "Books Mapping Not Changed";
    public static final String BOOKS_MAPPING_NOT_FOUND_STATE = "Books Mapping Not Found";

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
    public void testSuccessfulMappingRefresh() throws MalformedURLException, InterruptedException {
        setUpBooksMappingRefreshScenario();
        ExternalMappingSourceProcessor.Configuration config = new ExternalMappingSourceProcessor.Configuration(
            SOURCE_FIELD_NAME, TARGET_FIELD_NAME,
            "http://localhost:" + port + BOOKS_MAPPING, 50
        );
        ExternalMappingSourceProcessor processor = new ExternalMappingSourceProcessor(config);

        Doc doc = createDoc(SOURCE_FIELD_NAME, "Lewis Carroll");
        processor.process(doc);

        /* initial call returns mapping without Lewis Carroll */
        Iterable<String> targetField = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetField).isEmpty();

        await().atMost(1, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                /* second call returns mapping with Lewis Carroll */
                processor.process(doc);
                Iterable<String> targetFieldAfterRefresh = doc.getField(TARGET_FIELD_NAME);
                assertThat(targetFieldAfterRefresh).containsAll(Arrays.asList("Alice's Adventures in Wonderland", "Through the Looking-Glass"));
            });
    }

    @Test
    public void testProcessorRetainsPreviousMappingOnRefreshFailure() throws MalformedURLException, InterruptedException {
        setUpBooksMappingNotFoundAfterRefresh();
        ExternalMappingSourceProcessor.Configuration config = new ExternalMappingSourceProcessor.Configuration(
            SOURCE_FIELD_NAME, TARGET_FIELD_NAME,
            "http://localhost:" + port + BOOKS_MAPPING, DISABLE_MAPPING_REFRESH
        );
        ExternalMappingSourceProcessor processor = new ExternalMappingSourceProcessor(config);

        Doc doc1 = createDoc(SOURCE_FIELD_NAME, "Charles Dickens");
        /* invokes refreshExternalMapping() under the hood */
        processor.process(doc1);

        assertThat(doc1.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField1 = doc1.getField(TARGET_FIELD_NAME);
        assertThat(targetField1).containsAll(Arrays.asList("Oliver Twist", "A Christmas Carol", "The Chimes"));

        /* second call to the refresh mapping endpoint return 404 */
        processor.refreshExternalMapping();
        Doc doc2 = createDoc(SOURCE_FIELD_NAME, "Charles Dickens");
        processor.process(doc2);

        /* processor should retain previous mapping version regardless of refresh failure */
        assertThat(doc2.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField2 = doc2.getField(TARGET_FIELD_NAME);
        assertThat(targetField2).containsAll(Arrays.asList("Oliver Twist", "A Christmas Carol", "The Chimes"));
    }

    @Test
    public void testProcessorRetainsPreviousMappingOnNotModifiedResponse() throws MalformedURLException, InterruptedException {
        setUpBooksMappingNotModifiedScenario();

        ExternalMappingSourceProcessor.Configuration config = new ExternalMappingSourceProcessor.Configuration(
            SOURCE_FIELD_NAME, TARGET_FIELD_NAME,
            "http://localhost:" + port + BOOKS_MAPPING, DISABLE_MAPPING_REFRESH
        );
        ExternalMappingSourceProcessor processor = new ExternalMappingSourceProcessor(config);

        Doc doc1 = createDoc(SOURCE_FIELD_NAME, "Jack London");
        /* invokes refreshExternalMapping() under the hood */
        processor.process(doc1);

        assertThat(doc1.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField1 = doc1.getField(TARGET_FIELD_NAME);
        assertThat(targetField1).containsAll(Arrays.asList("White Fang", "Martin Eden", "The Sea Wolf"));

        /* second call to the refresh mapping endpoint return 304 */
        processor.refreshExternalMapping();
        Doc doc2 = createDoc(SOURCE_FIELD_NAME, "Jack London");
        processor.process(doc2);

        /* processor should retain previous mapping when GET external-mapping request return 304 status */
        assertThat(doc2.hasField(TARGET_FIELD_NAME)).isTrue();
        Iterable<String> targetField2 = doc2.getField(TARGET_FIELD_NAME);
        assertThat(targetField2).containsAll(Arrays.asList("White Fang", "Martin Eden", "The Sea Wolf"));

        /* error tags shouldn't be present in the document */
        assertThat(doc2.hasField("tags")).isFalse();
    }

    private void setUpBooksMappingRefreshScenario() {
        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario(MAPPING_REFRESH_SCENARIO)
            .whenScenarioStateIs(STARTED)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain; charset=utf-8")
                    .withHeader("Last-Modified",
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
                    )
                    .withBodyFile("books_mapping.properties")
            )
            .willSetStateTo(UPDATED_BOOKS_MAPPING_STATE)
        );

        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario(MAPPING_REFRESH_SCENARIO)
            .whenScenarioStateIs(UPDATED_BOOKS_MAPPING_STATE)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain; charset=utf-8")
                    .withHeader("Last-Modified",
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
                    )
                    .withBodyFile("updated_books_mapping.properties")
            )
        );
    }

    private void setUpBooksMappingNotFoundAfterRefresh() {
        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario(MAPPING_NOT_FOUND_AFTER_REFRESH_SCENARIO)
            .whenScenarioStateIs(STARTED)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain; charset=utf-8")
                    .withBodyFile("books_mapping.properties")
            )
            .willSetStateTo(BOOKS_MAPPING_NOT_FOUND_STATE)
        );

        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario(MAPPING_NOT_FOUND_AFTER_REFRESH_SCENARIO)
            .whenScenarioStateIs(BOOKS_MAPPING_NOT_FOUND_STATE)
            .willReturn(
                aResponse()
                    .withStatus(404)
                    .withBody("Not Found")
            )
        );
    }

    private void setUpBooksMappingNotModifiedScenario() {
        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario(MAPPING_NOT_MODIFIED_AFTER_REFRESH_SCENARIO)
            .whenScenarioStateIs(STARTED)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain; charset=utf-8")
                    .withBodyFile("books_mapping.properties")
            )
            .willSetStateTo(BOOKS_MAPPING_NOT_MODIFIED_STATE)
        );

        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario(MAPPING_NOT_MODIFIED_AFTER_REFRESH_SCENARIO)
            .whenScenarioStateIs(BOOKS_MAPPING_NOT_MODIFIED_STATE)
            .willReturn(
                aResponse()
                    .withStatus(304)
                    .withBody("Not Modified")
                    .withHeader("Last-Modified",
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
                    )
            )
        );
    }
}
