import io.logz.sawmill.Pipeline;
import io.logz.sawmill.ProcessorFactoriesLoader;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.processors.TestProcessor;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineTest {

    @Test
    public void testFactoryCreationWithJson() {
        String configJson = "{" +
                    "\"id\": \"abc\"," +
                    "\"name\": \"test pipeline\"," +
                    "\"description\": \"this is pipeline configuration\"," +
                    "\"processors\": [{" +
                        "\"name\": \"test\"," +
                        "\"config\": {" +
                            "\"value\": \"message\"" +
                        "}" +
                    "}]" +
                "}";

        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcesses(processorFactoryRegistry);
        Pipeline.Factory factory = new Pipeline.Factory(processorFactoryRegistry);
        Pipeline pipeline = factory.create(configJson);

        assertThat(pipeline.getId()).isEqualTo("abc");
        assertThat(pipeline.getName()).isEqualTo("test pipeline");
        assertThat(pipeline.getDescription()).isEqualTo("this is pipeline configuration");
        assertThat(pipeline.getProcessors().size()).isEqualTo(1);
        assertThat(pipeline.getProcessors().get(0).getName()).isEqualTo("test");
        assertThat(((TestProcessor)pipeline.getProcessors().get(0)).getValue()).isEqualTo("message");
    }

    @Test
    public void testFactoryCreationWithHocon() {
        String configHocon = "id : abc, name : hocon, description : this is hocon, processors: [{name:test,config.value:message}]";

        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcesses(processorFactoryRegistry);
        Pipeline.Factory factory = new Pipeline.Factory(processorFactoryRegistry);
        Pipeline pipeline = factory.create(configHocon);

        assertThat(pipeline.getId()).isEqualTo("abc");
        assertThat(pipeline.getDescription()).isEqualTo("this is hocon");
        assertThat(pipeline.getProcessors().size()).isEqualTo(1);
        assertThat(pipeline.getProcessors().get(0).getName()).isEqualTo("test");
        assertThat(((TestProcessor)pipeline.getProcessors().get(0)).getValue()).isEqualTo("message");
    }
}
