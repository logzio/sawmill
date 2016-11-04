import io.logz.sawmill.Pipeline;
import io.logz.sawmill.ProcessFactoriesLoader;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;
import processors.TestProcessor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineTest {

    @Test
    public void testFactoryCreation() {
        String configJson = "{" +
                    "\"id\": \"abc\"," +
                    "\"name\": \"test pipeline\"," +
                    "\"description\": \"this is pipeline configuration\"," +
                    "\"processors\": [{" +
                        "\"name\": \"test\"," +
                        "\"config\": {" +
                            "\"value\": \"message\"" +
                        "}" +
                    "},{" +
                            "\"name\": \"addField\"," +
                            "\"config\": {" +
                                "\"path\": \"path\"," +
                                "\"value\": \"sheker\"" +
                            "}" +
                    "}]" +
                "}";

        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        processorFactoryRegistry.register("test", new TestProcessor.Factory());
        ProcessFactoriesLoader.getInstance().loadAnnotatedProcesses(processorFactoryRegistry);
        Pipeline.Factory factory = new Pipeline.Factory(processorFactoryRegistry);
        Pipeline pipeline = factory.create(JsonUtils.fromJsonString(Pipeline.Configuration.class, configJson));

        assertThat(pipeline.getId()).isEqualTo("abc");
        assertThat(pipeline.getName()).isEqualTo("test pipeline");
        assertThat(pipeline.getDescription()).isEqualTo("this is pipeline configuration");
        assertThat(pipeline.getProcessors().size()).isEqualTo(2);
        assertThat(pipeline.getProcessors().get(0).getName()).isEqualTo("test");
        assertThat(pipeline.getProcessors().get(1).getName()).isEqualTo("addField");
        assertThat(((TestProcessor)pipeline.getProcessors().get(0)).getValue()).isEqualTo("message");
    }
}
