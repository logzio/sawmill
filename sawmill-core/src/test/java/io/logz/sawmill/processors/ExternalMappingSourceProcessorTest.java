package io.logz.sawmill.processors;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
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
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class ExternalMappingSourceProcessorTest {

    public static final String KEY_FIELD_NAME = "author";
    public static final String TARGET_FIELD_NAME = "books";

    private static WireMockServer wireMockServer;
    private static Integer port;

    @BeforeClass
    public static void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.stubFor(WireMock.get("/mappings").willReturn(
                ResponseDefinitionBuilder.like(ResponseDefinitionBuilder.responseDefinition().withBody(
                        "Charles Dickens = Oliver Twist, A Christmas Carol, The Chimes\n" +
                        "Jack London = White Fang, Martin Eden, Hearts of Three\n" +
                        "Ernest Hemingway = For Whom the Bell Tolls, A Farewell to Arms, The Old Man and the Sea\n"
                ).withStatus(200).build())));
        wireMockServer.start();
        port = wireMockServer.port();
    }

    @AfterClass
    public static void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testExternalMappingSourceProcessor() throws InterruptedException {
        ExternalMappingSourceProcessor processor = createProcessor();

        Doc firstDoc = createDoc(KEY_FIELD_NAME, "Charles Dickens");
        processor.process(firstDoc);

        assertThat(firstDoc.hasField(TARGET_FIELD_NAME)).isTrue();
        List<String> targetField1 = firstDoc.getField(TARGET_FIELD_NAME);
        assertThat(targetField1).containsAll(Arrays.asList("Oliver Twist", "A Christmas Carol", "The Chimes"));

        Doc secondDoc = createDoc(KEY_FIELD_NAME, "Ernest Hemingway");
        processor.process(secondDoc);

        assertThat(secondDoc.hasField(TARGET_FIELD_NAME)).isTrue();
        List<String> targetField2 = secondDoc.getField(TARGET_FIELD_NAME);
        assertThat(targetField2).containsAll(Arrays.asList("For Whom the Bell Tolls", "A Farewell to Arms", "The Old Man and the Sea"));
    }

    @Test
    public void testExternalMappingSourceProcessorMissingMapping() throws InterruptedException {
        ExternalMappingSourceProcessor processor = createProcessor();

        Doc doc = createDoc(KEY_FIELD_NAME, "Mark Twain");
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD_NAME)).isTrue();
        List<String> targetField = doc.getField(TARGET_FIELD_NAME);
        assertThat(targetField).isEmpty();
    }

    private ExternalMappingSourceProcessor createProcessor() {
        Map<String, Object> config = ImmutableMap.of(
                "keyFieldName", KEY_FIELD_NAME,
                "targetFieldName", TARGET_FIELD_NAME,
                "mappingSourceUrl", "http://localhost:" + port + "/mappings"
        );

        return FactoryUtils.createProcessor(ExternalMappingSourceProcessor.class, config);
    }
}
