import io.logz.sawmill.AnnotationLoaderProcessFactoryRegistry;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.ProcessFactoryRegistry;
import io.logz.sawmill.processes.TestProcess;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineTest {

    @Test
    public void testFactoryCreation() {
        String configJson = "{" +
                    "\"id\": \"abc\"," +
                    "\"description\": \"this is pipeline configuration\"," +
                    "\"processes\": [{" +
                        "\"name\": \"test\"," +
                        "\"config\": {" +
                            "\"value\": \"message\"" +
                        "}" +
                    "}]" +
                "}";
        ProcessFactoryRegistry processFactoryRegistry = new ProcessFactoryRegistry();
        AnnotationLoaderProcessFactoryRegistry.getInstance().loadAnnotatedProcesses(processFactoryRegistry);
        Pipeline.Factory factory = new Pipeline.Factory(processFactoryRegistry);
        Pipeline pipeline = factory.create(JsonUtils.fromJsonString(Pipeline.Configuration.class, configJson));

        assertThat(pipeline.getId()).isEqualTo("abc");
        assertThat(pipeline.getDescription()).isEqualTo("this is pipeline configuration");
        assertThat(pipeline.getProcesses().size()).isEqualTo(1);
        assertThat(pipeline.getProcesses().get(0).getName()).isEqualTo("test");
        assertThat(((TestProcess)pipeline.getProcesses().get(0)).getValue()).isEqualTo("message");
    }
}
