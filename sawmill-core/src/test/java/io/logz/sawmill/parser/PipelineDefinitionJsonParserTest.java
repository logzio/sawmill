package io.logz.sawmill.parser;

import org.junit.Test;

import java.util.List;

import static io.logz.sawmill.utilities.JsonUtils.createJson;
import static io.logz.sawmill.utilities.JsonUtils.createList;
import static io.logz.sawmill.utilities.JsonUtils.createMap;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineDefinitionJsonParserTest {

    @Test
    public void testJson() {
        String configJson = createJson(createMap(
                "steps", createList(
                        createMap("test", createMap(
                                "name", "test1",
                                "config", createMap(
                                        "value", "message"
                                )
                        ))
                )
        ));

        PipelineDefinition pipelineDefinition = PipelineDefinitionJsonParser.parse(configJson);
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipelineDefinition.isStopOnFailure().isPresent()).isFalse();

        ProcessorExecutionStepDefinition processorExecutionStepDefinition =
                (ProcessorExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        assertThat(processorExecutionStepDefinition.getName().get()).isEqualTo("test1");
        ProcessorDefinition processorDefinition = processorExecutionStepDefinition.getProcessorDefinition();
        assertThat(processorDefinition.getType()).isEqualTo("test");
        assertThat(processorDefinition.getConfig().get("value")).isEqualTo("message");

        assertThat(processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList().isPresent()).isFalse();
    }

    @Test
    public void testHocon() {
        String configHocon =
                        "steps: [" +
                        "    {" +
                        "        test: {" +
                        "            config.value: message" +
                        "        }" +
                        "    }" +
                        "]";

        PipelineDefinition pipelineDefinition = PipelineDefinitionJsonParser.parse(configHocon);
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipelineDefinition.isStopOnFailure().isPresent()).isFalse();

        ProcessorExecutionStepDefinition processorExecutionStepDefinition = (ProcessorExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        assertThat(processorExecutionStepDefinition.getName().isPresent()).isFalse();

        ProcessorDefinition processorDefinition = processorExecutionStepDefinition.getProcessorDefinition();
        assertThat(processorDefinition.getType()).isEqualTo("test");
        assertThat(processorDefinition.getConfig().get("value")).isEqualTo("message");

        assertThat(processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList().isPresent()).isFalse();
    }

    @Test
    public void testOnFailure() {
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
                "stopOnFailure", false
        ));

        PipelineDefinition pipelineDefinition = PipelineDefinitionJsonParser.parse(configJson);
        assertThat(pipelineDefinition.isStopOnFailure().get()).isFalse();
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);

        ProcessorExecutionStepDefinition processorExecutionStepDefinition =
                (ProcessorExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        ProcessorDefinition config = processorExecutionStepDefinition.getProcessorDefinition();
        assertThat(config.getType()).isEqualTo("test");
        assertThat(config.getConfig().get("value")).isEqualTo("message");

        List<ExecutionStepDefinition> onFailureExecutionStepDefinitions
                = processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList().get();
        assertThat(onFailureExecutionStepDefinitions.size()).isEqualTo(1);

        ProcessorExecutionStepDefinition onFailureExecutionStepDefinition = (ProcessorExecutionStepDefinition) onFailureExecutionStepDefinitions.get(0);
        assertThat(onFailureExecutionStepDefinition.getName().get()).isEqualTo("on failure processor");

        ProcessorDefinition processorDefinition = onFailureExecutionStepDefinition.getProcessorDefinition();
        assertThat(processorDefinition.getType()).isEqualTo("addField");
        assertThat(processorDefinition.getConfig().get("path")).isEqualTo("field1");
        assertThat(processorDefinition.getConfig().get("value")).isEqualTo("value1");
    }

    @Test
    public void testConditional() {
        String json = createJson(createMap(
                "steps", createList(
                        createMap(
                                "if", createMap(
                                        "condition", createMap(
                                                "and", createList(
                                                        createMap("testCondition", createMap(
                                                                "value", "message1"
                                                        )),
                                                        createMap("testCondition", createMap(
                                                                "value", "message2"
                                                        ))
                                                )
                                        ),
                                        "then", createList(
                                                createMap("removeField", createMap(
                                                        "name", "on true",
                                                        "config", createMap("path", "field1")
                                                ))

                                        )
                                ))
                )
        ));

        PipelineDefinition pipelineDefinition = PipelineDefinitionJsonParser.parse(json);
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);

        ConditionalExecutionStepDefinition executionStepDefinition =
                (ConditionalExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        ConditionDefinition conditionDefinition = executionStepDefinition.getConditionDefinition();

        assertThat(conditionDefinition.getType()).isEqualTo("and");
        List<ConditionDefinition> andConditions = (List<ConditionDefinition>) conditionDefinition.getConfig().get("conditions");
        assertThat(andConditions.size()).isEqualTo(2);
        ConditionDefinition andCondition1 = andConditions.get(0);

        assertThat(andCondition1.getType()).isEqualTo("testCondition");
        assertThat(andCondition1.getConfig().get("value")).isEqualTo("message1");

        ConditionDefinition andCondition2 = andConditions.get(1);
        assertThat(andCondition2.getType()).isEqualTo("testCondition");
        assertThat(andCondition2.getConfig().get("value")).isEqualTo("message2");

        List<ExecutionStepDefinition> onTrue = executionStepDefinition.getOnTrue();
        assertThat(onTrue.size()).isEqualTo(1);

        ProcessorExecutionStepDefinition onTrueProcessorExecutionStep = (ProcessorExecutionStepDefinition) onTrue.get(0);
        assertThat(onTrueProcessorExecutionStep.getName().get()).isEqualTo("on true");
        assertThat(onTrueProcessorExecutionStep.getOnFailureExecutionStepDefinitionList().isPresent()).isFalse();

        ProcessorDefinition processorDefinition = onTrueProcessorExecutionStep.getProcessorDefinition();
        assertThat(processorDefinition.getType()).isEqualTo("removeField");
        assertThat(processorDefinition.getConfig().get("path")).isEqualTo("field1");

        assertThat(executionStepDefinition.getOnFalse().isPresent()).isFalse();
    }

    @Test
    public void testConditionalElse() {
        String json = createJson(createMap(
                "steps", createList(
                        createMap(
                                "if", createMap(
                                        "condition", createMap(
                                                "testCondition", createMap("value", "message1")),
                                        "then", createList(
                                                createMap("removeField", createMap(
                                                        "name", "on true",
                                                        "config", createMap("path", "field1")
                                                ))
                                        ),
                                        "else", createList(
                                                createMap("removeField", createMap(
                                                        "name", "on false",
                                                        "config", createMap("path", "field2")
                                                ))
                                        )
                                ))
                )
        ));

        PipelineDefinition pipelineDefinition = PipelineDefinitionJsonParser.parse(json);
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);

        ConditionalExecutionStepDefinition executionStepDefinition =
                (ConditionalExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);

        List<ExecutionStepDefinition> onFalse = executionStepDefinition.getOnFalse().get();
        assertThat(onFalse.size()).isEqualTo(1);

        ProcessorExecutionStepDefinition onFalseProcessorExecutionStep = (ProcessorExecutionStepDefinition) onFalse.get(0);
        assertThat(onFalseProcessorExecutionStep.getName().get()).isEqualTo("on false");
        assertThat(onFalseProcessorExecutionStep.getOnFailureExecutionStepDefinitionList().isPresent()).isFalse();

        ProcessorDefinition processorDefinition2 = onFalseProcessorExecutionStep.getProcessorDefinition();
        assertThat(processorDefinition2.getType()).isEqualTo("removeField");
        assertThat(processorDefinition2.getConfig().get("path")).isEqualTo("field2");
    }



}