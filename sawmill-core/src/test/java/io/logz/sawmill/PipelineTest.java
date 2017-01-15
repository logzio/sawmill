package io.logz.sawmill;

import io.logz.sawmill.conditions.AndCondition;
import io.logz.sawmill.conditions.TestCondition;
import io.logz.sawmill.processors.TestProcessor;
import org.junit.Before;
import org.junit.Test;

import static io.logz.sawmill.utilities.JsonUtils.createJson;
import static io.logz.sawmill.utilities.JsonUtils.createList;
import static io.logz.sawmill.utilities.JsonUtils.createMap;
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
        String configJson = createJson(createMap(
                "steps", createList(createMap(
                        "test", createMap(
                                "name", "test1",
                                "config", createMap("value", "message"),
                                "onFailure", createList(createMap(
                                        "addField", createMap(
                                                "name", "on failure processor",
                                                "config", createMap(
                                                        "path", "field1",
                                                        "value", "value1"
                                                )
                                        )
                                ))
                        )
                )),
                "ignoreFailure", false
        ));

        String id = "abc";
        Pipeline pipeline = factory.create(id, configJson);

        assertThat(pipeline.getId()).isEqualTo(id);
        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        ProcessorExecutionStep executionStep = (ProcessorExecutionStep) pipeline.getExecutionSteps().get(0);
        TestProcessor processor = (TestProcessor) executionStep.getProcessor();
        assertThat(executionStep.getProcessorName()).isEqualTo("test1");
        assertThat(processor.getValue()).isEqualTo("message");
        assertThat(pipeline.isIgnoreFailure()).isFalse();
        assertThat(executionStep.getOnFailureExecutionSteps().get().size()).isEqualTo(1);
    }

    @Test
    public void testFactoryCreationHoconWithoutId() {
        String configHocon =
                        "steps: [" +
                        "    {" +
                        "        test: {" +
                        "            name: test1, " +
                        "            config.value: message" +
                        "        }" +
                        "    }" +
                        "]";

        String id = "abc";
        Pipeline pipeline = factory.create(id, configHocon);

        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipeline.isIgnoreFailure()).isTrue();

        ProcessorExecutionStep executionStep = (ProcessorExecutionStep) pipeline.getExecutionSteps().get(0);
        Processor processor = executionStep.getProcessor();
        assertThat(executionStep.getProcessorName()).isEqualTo("test1");
        assertThat(((TestProcessor) processor).getValue()).isEqualTo("message");
    }

    @Test
    public void testConditional() {
        String pipelineString = "{" +
                "steps: [{" +
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
                "        }]" +
                "    }" +
                "}]" +
                "}";

        String id = "abc";
        Pipeline pipeline = factory.create(id, pipelineString);

        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipeline.isIgnoreFailure()).isTrue();

        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) pipeline.getExecutionSteps().get(0);

        assertThat(conditionalExecutionStep.getCondition()).isInstanceOf(AndCondition.class);

        ProcessorExecutionStep onTrueExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnTrue().get(0);
        assertThat(onTrueExecutionStep.getProcessorName()).isEqualTo("processor1");
        assertThat(conditionalExecutionStep.getOnFalse().size()).isEqualTo(0);
    }

    @Test
    public void testConditionalElse() {
        String pipelineString = "{" +
                "steps: [{" +
                "    if: {" +
                "        condition: {" +
                "            testCondition.value: message1" +
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
        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) pipeline.getExecutionSteps().get(0);

        ProcessorExecutionStep onFalseExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnFalse().get(0);
        assertThat(onFalseExecutionStep.getProcessorName()).isEqualTo("processor2");
    }
}
