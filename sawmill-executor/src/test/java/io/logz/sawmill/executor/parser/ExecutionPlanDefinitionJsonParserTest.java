package io.logz.sawmill.executor.parser;

import org.junit.Before;
import org.junit.Test;

import static io.logz.sawmill.utilities.JsonUtils.createJson;
import static io.logz.sawmill.utilities.JsonUtils.createList;
import static io.logz.sawmill.utilities.JsonUtils.createMap;
import static org.assertj.core.api.Assertions.assertThat;

public class ExecutionPlanDefinitionJsonParserTest {
    private ExecutionPlanDefinitionJsonParser executionPlanDefinitionJsonParser;

    @Before
    public void init() {
        executionPlanDefinitionJsonParser = new ExecutionPlanDefinitionJsonParser();
    }

    @Test
    public void testJson() {
        String config = createJson(createMap(
                "input", createMap("testInput", createMap(
                                "value", "message"
                )),
                "steps", createList(
                        createMap("test", createMap(
                                "name", "test1",
                                "config", createMap(
                                        "value", "message"
                                )
                        ))
                ),
                "output", createMap("testOutput", createMap(
                                "value", "message"
                ))
        ));

        ExecutionPlanDefinition executionPlanDefinition = executionPlanDefinitionJsonParser.parse(config);

        InputDefinition inputDefinition = executionPlanDefinition.getInputDefinition();
        assertThat(inputDefinition.getType()).isEqualTo("testInput");
        assertThat(inputDefinition.getConfig().size()).isEqualTo(1);
        assertThat(inputDefinition.getConfig().get("value")).isEqualTo("message");

        OutputDefinition outputDefinition = executionPlanDefinition.getOutputDefinition();
        assertThat(outputDefinition.getType()).isEqualTo("testOutput");
        assertThat(outputDefinition.getConfig().size()).isEqualTo(1);
        assertThat(outputDefinition.getConfig().get("value")).isEqualTo("message");
    }

    @Test
    public void testHocon() {
        String config =
                "input: {" +
                "   testInput.value: message" +
                "}," +
                "steps: [{" +
                "   test: {" +
                "       config.value: message" +
                "   }" +
                "}]," +
                "output: {" +
                "   testOutput.value: message" +
                "}";
        ExecutionPlanDefinition executionPlanDefinition = executionPlanDefinitionJsonParser.parse(config);

        InputDefinition inputDefinition = executionPlanDefinition.getInputDefinition();
        assertThat(inputDefinition.getType()).isEqualTo("testInput");
        assertThat(inputDefinition.getConfig().size()).isEqualTo(1);
        assertThat(inputDefinition.getConfig().get("value")).isEqualTo("message");

        OutputDefinition outputDefinition = executionPlanDefinition.getOutputDefinition();
        assertThat(outputDefinition.getType()).isEqualTo("testOutput");
        assertThat(outputDefinition.getConfig().size()).isEqualTo(1);
        assertThat(outputDefinition.getConfig().get("value")).isEqualTo("message");

    }
}
