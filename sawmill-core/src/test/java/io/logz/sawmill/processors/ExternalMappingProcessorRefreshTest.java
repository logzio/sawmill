package io.logz.sawmill.processors;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import io.logz.sawmill.Doc;
import java.net.MalformedURLException;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class ExternalMappingProcessorRefreshTest {

    public static final String SOURCE_FIELD_NAME = "author";
    public static final String TARGET_FIELD_NAME = "books";

    public static final String BOOKS_MAPPING = "/books";

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
    public void testMappingRefresh() throws MalformedURLException, InterruptedException {
        setUpBooksMappingRefreshScenario();
        ExternalMappingSourceProcessor.Configuration config =
            new ExternalMappingSourceProcessor.Configuration(
                SOURCE_FIELD_NAME, TARGET_FIELD_NAME, "http://localhost:" + port + BOOKS_MAPPING,
                50, 5000, 10000
            );
        ExternalMappingSourceProcessor processor = new ExternalMappingSourceProcessor(config);

        Doc doc = createDoc(SOURCE_FIELD_NAME, "Lewis Carroll");
        processor.process(doc);

        /* initial call returns mapping without Lewis Carroll */
        Iterable<String> targetField = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetField).isEmpty();

        Thread.sleep(300);

        /* second call returns mapping with Lewis Carroll */
        processor.process(doc);
        Iterable<String> targetFieldAfterRefresh = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetFieldAfterRefresh).containsAll(Arrays.asList("Alice's Adventures in Wonderland", "Through the Looking-Glass"));

        Thread.sleep(300);

        /* third call returns 404 and should use previous mapping */
        processor.process(doc);
        Iterable<String> targetFieldAfterSecondRefresh = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetFieldAfterSecondRefresh).containsAll(Arrays.asList("Alice's Adventures in Wonderland", "Through the Looking-Glass"));
    }

    private void setUpBooksMappingRefreshScenario() {
        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario("Books Mapping Refresh")
            .whenScenarioStateIs(STARTED)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain; charset=utf-8")
                    .withBodyFile("books_mapping.properties")
            )
            .willSetStateTo("Updated Books Mapping")
        );

        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario("Books Mapping Refresh")
            .whenScenarioStateIs("Updated Books Mapping")
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain; charset=utf-8")
                    .withBodyFile("updated_books_mapping.properties")
            )
            .willSetStateTo("Books Mapping Not Found")
        );

        wireMockRule.stubFor(get(BOOKS_MAPPING).inScenario("Books Mapping Refresh")
            .whenScenarioStateIs("Books Mapping Not Found")
            .willReturn(
                aResponse()
                    .withStatus(404)
                    .withBody("Not Found")
            )
        );
    }
}
