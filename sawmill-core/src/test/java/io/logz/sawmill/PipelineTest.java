package io.logz.sawmill;

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
                        "\"type\": \"test\"," +
                        "\"name\": \"test1\"," +
                        "\"config\": {" +
                            "\"value\": \"message\"" +
                        "}," +
                        "\"onFailure\": [{" +
                            "\"type\": \"addField\"," +
                            "\"name\": \"addField1\"," +
                            "\"config\": {" +
                                "\"path\": \"path\"," +
                                "\"value\": \"sheker\"" +
                            "}" +
                        "}]" +
                    "}]," +
                    "\"ignoreFailure\": \"true\"" +
                "}";

        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        processorFactoryRegistry.register("test", new TestProcessor.Factory());
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcesses(processorFactoryRegistry);
        Pipeline.Factory factory = new Pipeline.Factory(processorFactoryRegistry);
        Pipeline pipeline = factory.create(configJson);

        assertThat(pipeline.getId()).isEqualTo("abc");
        assertThat(pipeline.getName()).isEqualTo("test pipeline");
        assertThat(pipeline.getDescription()).isEqualTo("this is pipeline configuration");
        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        ExecutionStep executionStep = pipeline.getExecutionSteps().get(0);
        TestProcessor processor = (TestProcessor) executionStep.getProcessor();
        assertThat(processor.getType()).isEqualTo("test");
        assertThat(processor.getValue()).isEqualTo("message");
        assertThat(pipeline.isIgnoreFailure()).isTrue();
        assertThat(executionStep.getOnFailureProcessors().size()).isEqualTo(1);
        assertThat(executionStep.getOnFailureProcessors().get(0).getType()).isEqualTo("addField");
    }

    @Test
    public void testFactoryCreationWithHocon() {
        String configHocon = "id : abc, name : hocon, description : this is hocon, processors: [{type:test, name:test1, config.value:message}]";

        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        processorFactoryRegistry.register("test", new TestProcessor.Factory());
        Pipeline.Factory factory = new Pipeline.Factory(processorFactoryRegistry);
        Pipeline pipeline = factory.create(configHocon);

        assertThat(pipeline.getId()).isEqualTo("abc");
        assertThat(pipeline.getDescription()).isEqualTo("this is hocon");
        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        Processor processor = pipeline.getExecutionSteps().get(0).getProcessor();
        assertThat(processor.getType()).isEqualTo("test");
        assertThat(((TestProcessor) processor).getValue()).isEqualTo("message");
    }
}
