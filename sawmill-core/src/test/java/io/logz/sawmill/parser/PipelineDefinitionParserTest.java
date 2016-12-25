package io.logz.sawmill.parser;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.logz.sawmill.JsonUtils.createJson;
import static io.logz.sawmill.JsonUtils.createList;
import static io.logz.sawmill.JsonUtils.createMap;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Created by naorguetta on 22/12/2016.
 */
public class PipelineDefinitionParserTest {

    private PipelineDefinitionParser pipelineDefinitionParser;

    @Before
    public void init() {
        pipelineDefinitionParser = new PipelineDefinitionParser();
    }

    @Test
    public void testJson() {
        String configJson = createJson(createMap(
                "description", "this is hocon",
                "name", "json",
                "executionSteps", createList(
                        createMap("test", createMap(
                                "name", "test1",
                                "config", createMap(
                                        "value", "message"
                                )
                        ))
                )
        ));

        PipelineDefinition pipelineDefinition = pipelineDefinitionParser.parse(configJson);
        assertThat(pipelineDefinition.getName()).isEqualTo("json");
        assertThat(pipelineDefinition.getDescription()).isEqualTo("this is hocon");
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipelineDefinition.isIgnoreFailure()).isNull();

        ProcessorExecutionStepDefinition processorExecutionStepDefinition = (ProcessorExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        assertThat(processorExecutionStepDefinition.getName()).isEqualTo("test1");
        ProcessorDefinition processorDefinition = processorExecutionStepDefinition.getProcessorDefinition();
        assertThat(processorDefinition.getType()).isEqualTo("test");
        assertThat(processorDefinition.getConfig().get("value")).isEqualTo("message");

        assertThat(processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList()).isNull();
    }

    @Test
    public void testHocon() {
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

        PipelineDefinition pipelineDefinition = pipelineDefinitionParser.parse(configHocon);
        assertThat(pipelineDefinition.getName()).isEqualTo("hocon");
        assertThat(pipelineDefinition.getDescription()).isEqualTo("this is hocon");
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);
        assertThat(pipelineDefinition.isIgnoreFailure()).isNull();

        ProcessorExecutionStepDefinition processorExecutionStepDefinition = (ProcessorExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        assertThat(processorExecutionStepDefinition.getName()).isEqualTo("test1");

        ProcessorDefinition processorDefinition = processorExecutionStepDefinition.getProcessorDefinition();
        assertThat(processorDefinition.getType()).isEqualTo("test");
        assertThat(processorDefinition.getConfig().get("value")).isEqualTo("message");

        assertThat(processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList()).isNull();
    }

    @Test
    public void testOnFailure() {
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
                "    \"ignoreFailure\": false" +
                "}";

        PipelineDefinition pipelineDefinition = pipelineDefinitionParser.parse(configJson);
        assertThat(pipelineDefinition.getName()).isEqualTo("test pipeline");
        assertThat(pipelineDefinition.getDescription()).isEqualTo("this is pipeline configuration");
        assertThat(pipelineDefinition.isIgnoreFailure()).isFalse();
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);

        ProcessorExecutionStepDefinition processorExecutionStepDefinition = (ProcessorExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        ProcessorDefinition config = processorExecutionStepDefinition.getProcessorDefinition();
        assertThat(config.getType()).isEqualTo("test");
        assertThat(config.getConfig().get("value")).isEqualTo("message");

        List<OnFailureExecutionStepDefinition> onFailureExecutionStepDefinitionList = processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList();
        assertThat(onFailureExecutionStepDefinitionList.size()).isEqualTo(1);

        OnFailureExecutionStepDefinition onFailureExecutionStepDefinition = onFailureExecutionStepDefinitionList.get(0);
        assertThat(onFailureExecutionStepDefinition.getName()).isEqualTo("addField1");

        ProcessorDefinition processorDefinition = onFailureExecutionStepDefinition.getProcessorDefinition();
        assertThat(processorDefinition.getType()).isEqualTo("addField");
        assertThat(processorDefinition.getConfig().get("path")).isEqualTo("path");
        assertThat(processorDefinition.getConfig().get("value")).isEqualTo("sheker");
    }

    @Test
    public void testIf() {
        String pipelineName = "pipeline1";
        String pipelineDescription = "description la la la";
        String json = createJson(createMap(
                "name", pipelineName,
                "description", pipelineDescription,
                "executionSteps", createList(
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
                                                        "name", "processor1",
                                                        "config", createMap("path", "field1")
                                                ))

                                        ),
                                        "else", createList(
                                                createMap("removeField", createMap(
                                                        "name", "processor2",
                                                        "config", createMap("path", "field2")
                                                ))
                                        )
                                ))
                )
        ));

        PipelineDefinition pipelineDefinition = pipelineDefinitionParser.parse(json);
        assertThat(pipelineDefinition.getName()).isEqualTo(pipelineName);
        assertThat(pipelineDefinition.getDescription()).isEqualTo(pipelineDescription);
        assertThat(pipelineDefinition.getExecutionSteps().size()).isEqualTo(1);

        ConditionalExecutionStepDefinition executionStepDefinition = (ConditionalExecutionStepDefinition) pipelineDefinition.getExecutionSteps().get(0);
        ConditionDefinition conditionDefinition = executionStepDefinition.getCondition();

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
        assertThat(onTrueProcessorExecutionStep.getName()).isEqualTo("processor1");
        assertThat(onTrueProcessorExecutionStep.getOnFailureExecutionStepDefinitionList()).isNull();

        ProcessorDefinition processorDefinition1 = onTrueProcessorExecutionStep.getProcessorDefinition();
        assertThat(processorDefinition1.getType()).isEqualTo("removeField");
        assertThat(processorDefinition1.getConfig().get("path")).isEqualTo("field1");

        List<ExecutionStepDefinition> onFalse = executionStepDefinition.getOnFalse();
        assertThat(onFalse.size()).isEqualTo(1);

        ProcessorExecutionStepDefinition onFalseProcessorExecutionStep = (ProcessorExecutionStepDefinition) onFalse.get(0);
        assertThat(onFalseProcessorExecutionStep.getName()).isEqualTo("processor2");
        assertThat(onFalseProcessorExecutionStep.getOnFailureExecutionStepDefinitionList()).isNull();

        ProcessorDefinition processorDefinition2 = onFalseProcessorExecutionStep.getProcessorDefinition();
        assertThat(processorDefinition2.getType()).isEqualTo("removeField");
        assertThat(processorDefinition2.getConfig().get("path")).isEqualTo("field2");

    }

}