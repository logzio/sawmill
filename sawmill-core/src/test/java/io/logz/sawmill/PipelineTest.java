package io.logz.sawmill;

import io.logz.sawmill.conditions.AndCondition;
import io.logz.sawmill.conditions.TestCondition;
import io.logz.sawmill.processors.AddTagProcessor;
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
        ConditionalFactoriesLoader.getInstance().loadAnnotatedConditions(conditionFactoryRegistry);

        factory = new Pipeline.Factory(processorFactoryRegistry, conditionFactoryRegistry);
    }

    @Test
    public void testFactoryCreationJson() {
        String configJson = createJson(createMap(
                "steps", createList(createMap(
                        "test", createMap(
                                "config", createMap("value", "message"),
                                "onFailure", createList(createMap(
                                        "addField", createMap(
                                                "config", createMap(
                                                        "path", "field1",
                                                        "value", "value1"
                                                )
                                        )
                                ))
                        )
                )),
                "stopOnFailure", true
        ));

        String id = "abc";
        Pipeline pipeline = factory.create(id, configJson);

        assertThat(pipeline.getId()).isEqualTo(id);
        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        ProcessorExecutionStep executionStep = (ProcessorExecutionStep) pipeline.getExecutionSteps().get(0);
        TestProcessor processor = (TestProcessor) executionStep.getProcessor();
        assertThat(processor.getValue()).isEqualTo("message");
        assertThat(pipeline.isStopOnFailure()).isTrue();
        assertThat(executionStep.getOnFailureExecutionSteps().get().size()).isEqualTo(1);
    }

    @Test
    public void testFactoryCreationHoconWithoutId() {
        String configHocon =
                        "steps: [" +
                        "    {" +
                        "        test: {" +
                        "            config.value: message" +
                        "        }" +
                        "    }" +
                        "]";

        String id = "abc";
        Pipeline pipeline = factory.create(id, configHocon);

        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipeline.isStopOnFailure()).isFalse();

        ProcessorExecutionStep executionStep = (ProcessorExecutionStep) pipeline.getExecutionSteps().get(0);
        Processor processor = executionStep.getProcessor();
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
                "                config.tags: [tag1, tag2]" +
                "            }" +
                "        }]" +
                "    }" +
                "}]" +
                "}";

        String id = "abc";
        Pipeline pipeline = factory.create(id, pipelineString);

        assertThat(pipeline.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipeline.isStopOnFailure()).isFalse();

        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) pipeline.getExecutionSteps().get(0);

        assertThat(conditionalExecutionStep.getCondition()).isInstanceOf(AndCondition.class);

        ProcessorExecutionStep onTrueExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnTrue().get(0);
        assertThat(onTrueExecutionStep.getProcessor()).isInstanceOf(AddTagProcessor.class);

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
                "            addTag.config.tags: [tag1, tag2]" +
                "        }]," +
                "        else: [{" +
                "            addTag.config.tags: [tag3, tag4]" +
                "        }]" +
                "    }" +
                "}]" +
                "}";

        String id = "abc";
        Pipeline pipeline = factory.create(id, pipelineString);
        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) pipeline.getExecutionSteps().get(0);
        assertThat(conditionalExecutionStep.getCondition()).isInstanceOf(TestCondition.class);

        ProcessorExecutionStep onFalseExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnFalse().get(0);
        assertThat(onFalseExecutionStep.getProcessor()).isInstanceOf(AddTagProcessor.class);
    }
}
