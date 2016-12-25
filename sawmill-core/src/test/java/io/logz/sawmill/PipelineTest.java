package io.logz.sawmill;

import io.logz.sawmill.conditions.AndCondition;
import io.logz.sawmill.conditions.TestCondition;
import io.logz.sawmill.processors.TestProcessor;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineTest {

    private ProcessorFactoryRegistry processorFactoryRegistry;
    private ConditionFactoryRegistry conditionFactoryRegistry;
    private Pipeline.Factory factory;

    @Before
    public void init() {
        processorFactoryRegistry = new ProcessorFactoryRegistry();
        processorFactoryRegistry.register("test", new TestProcessor.Factory());
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcessors(processorFactoryRegistry);

        conditionFactoryRegistry = new ConditionFactoryRegistry();
        conditionFactoryRegistry.register("testCondition", new TestCondition.Factory());
        ConditionalFactoriesLoader.getInstance().loadAnnotatedProcessors(conditionFactoryRegistry);

        factory = new Pipeline.Factory(processorFactoryRegistry, conditionFactoryRegistry);
    }

    @Test
    public void testFactoryCreationJson() {
        String configJson = "{" +
                "    \"name\": \"test pipeline\"," +
                "    \"description\": \"this is pipeline configuration\"," +
                "    \"executionSteps\": [{" +
                "        \"test\": {" +
                "            \"name\": \"test1\"," +
                "            \"config\": {" +
                "                \"value\": \"message\"" +
                "            }," +
                "            \"onFailure\": [{" +
                "                \"addField\": {" +
                "                    \"name\": \"addField1\"," +
                "                    \"config\": {" +
                "                        \"path\": \"path\"," +
                "                        \"value\": \"sheker\"" +
                "                    }" +
                "                }" +
                "            }]" +
                "        }" +
                "    }]," +
                "    \"ignoreFailure\": true" +
                "}";

        String id = "abc";
        Pipeline pipeline = factory.create(id, configJson);

        assertThat(pipeline.getId()).isEqualTo(id);
        assertThat(pipeline.getName()).isEqualTo("test pipeline");
        assertThat(pipeline.getDescription()).isEqualTo("this is pipeline configuration");
        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        ProcessorExecutionStep executionStep = (ProcessorExecutionStep) pipeline.getExecutionSteps().get(0);
        TestProcessor processor = (TestProcessor) executionStep.getProcessor();
        assertThat(executionStep.getProcessorName()).isEqualTo("test1");
        assertThat(processor.getValue()).isEqualTo("message");
        assertThat(pipeline.isIgnoreFailure()).isTrue();
        assertThat(executionStep.getOnFailureExecutionSteps().get().size()).isEqualTo(1);
    }

    @Test
    public void testFactoryCreationHoconWithoutId() {
        String configHocon =
                "name : hocon," +
                        "description : this is hocon, " +
                        "executionSteps: [" +
                        "    {" +
                        "        test: {" +
                        "            name: test1, " +
                        "            config.value: message" +
                        "        }" +
                        "    }" +
                        "]";

        String id = "abc";
        Pipeline pipeline = factory.create(id, configHocon);

        assertThat(pipeline.getDescription()).isEqualTo("this is hocon");
        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipeline.isIgnoreFailure()).isTrue();

        ProcessorExecutionStep executionStep = (ProcessorExecutionStep) pipeline.getExecutionSteps().get(0);
        Processor processor = executionStep.getProcessor();
        assertThat(executionStep.getProcessorName()).isEqualTo("test1");
        assertThat(((TestProcessor) processor).getValue()).isEqualTo("message");
    }

    @Test
    public void testIf() {
        String pipelineString = "{" +
                "name: pipeline1," +
                "description: description la la la," +
                "executionSteps: [{" +
                "    if: {" +
                "        condition: {" +
                "            and: [" +
                "                {" +
                "                    testCondition.value: message1" +
                "                }, " +
                "                {" +
                "                    testCondition.value: message2" +
                "                }" +
                "            ]" +
                "        }," +
                "        then: [{" +
                "            addTag: {" +
                "                name: processor1," +
                "                config.tags: [tag1, tag2]" +
                "            }" +
                "        }]," +
                "        else: [{" +
                "            addTag: {" +
                "                name: processor2," +
                "                config.tags: [tag3, tag4]" +
                "            }" +
                "        }]" +
                "    }" +
                "}]" +
                "}";

        String id = "abc";
        Pipeline pipeline = factory.create(id, pipelineString);

        assertThat(pipeline.getName()).isEqualTo("pipeline1");
        assertThat(pipeline.getDescription()).isEqualTo("description la la la");
        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipeline.isIgnoreFailure()).isTrue();

        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) pipeline.getExecutionSteps().get(0);

        AndCondition andCondition = (AndCondition) conditionalExecutionStep.getCondition();

        TestCondition testCondition1 = (TestCondition) andCondition.getConditions().get(0);
        assertThat(testCondition1.getValue()).isEqualTo("message1");
        TestCondition testCondition2 = (TestCondition) andCondition.getConditions().get(1);
        assertThat(testCondition2.getValue()).isEqualTo("message2");

        ProcessorExecutionStep onTrueExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnTrue().get(0);
        assertThat(onTrueExecutionStep.getProcessorName()).isEqualTo("processor1");
        ProcessorExecutionStep onFalseExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnFalse().get(0);
        assertThat(onFalseExecutionStep.getProcessorName()).isEqualTo("processor2");

    }
}
