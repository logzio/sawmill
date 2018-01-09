package io.logz.sawmill.executor;

import io.logz.sawmill.executor.inputs.TestInput;
import io.logz.sawmill.executor.outputs.TestOutput;
import org.junit.Before;
import org.junit.Test;

import static io.logz.sawmill.utilities.JsonUtils.createJson;
import static io.logz.sawmill.utilities.JsonUtils.createList;
import static io.logz.sawmill.utilities.JsonUtils.createMap;
import static org.assertj.core.api.Assertions.assertThat;

public class ExecutionPlanTest {
    private InputFactoryRegistry inputFactoryRegistry;
    private OutputFactoryRegistry outputFactoryRegistry;
    private ExecutionPlan.Factory factory;

    @Before
    public void init() {
        inputFactoryRegistry = new InputFactoryRegistry();
        inputFactoryRegistry.register("testInput", new TestInput.Factory());
        InputFactoriesLoader.getInstance().loadAnnotatedInputs(inputFactoryRegistry);

        outputFactoryRegistry = new OutputFactoryRegistry();
        outputFactoryRegistry.register("testOutput", new TestOutput.Factory());
        OutputFactoriesLoader.getInstance().loadAnnotatedOutputs(outputFactoryRegistry);

        factory = new ExecutionPlan.Factory(inputFactoryRegistry, outputFactoryRegistry);
    }

    @Test
    public void testFactoryCreationJson() {
        String config = createJson(createMap(
                "input", createMap("testInput", createMap(
                        "value", "message"
                )),
                "steps", createList(
                        createMap("addField", createMap(
                                "name", "test1",
                                "config", createMap(
                                        "path", "message",
                                        "value", "value1"
                                )
                        ))
                ),
                "output", createMap("testOutput", createMap(
                        "value", "message"
                ))
        ));

        ExecutionPlan executionPlan = factory.create(config);

        TestInput input = (TestInput) executionPlan.getInput();
        assertThat(input.getValue()).isEqualTo("message");
        TestOutput output = (TestOutput) executionPlan.getOutput();
        assertThat(output.getValue()).isEqualTo("message");
        assertThat(executionPlan.getPipeline().getExecutionSteps().size()).isEqualTo(1);
    }

    @Test
    public void testFactoryCreationHocon() {
        String config =
                "input: {" +
                "   testInput.value: message" +
                "}," +
                "steps: [{" +
                "   addField: {" +
                "       config: { " +
                "           path: message," +
                "           value: value1" +
                "       }" +
                "   }" +
                "}]," +
                "output: {" +
                "   testOutput.value: message" +
                "}";

        ExecutionPlan executionPlan = factory.create(config);

        TestInput input = (TestInput) executionPlan.getInput();
        assertThat(input.getValue()).isEqualTo("message");
        TestOutput output = (TestOutput) executionPlan.getOutput();
        assertThat(output.getValue()).isEqualTo("message");
        assertThat(executionPlan.getPipeline().getExecutionSteps().size()).isEqualTo(1);
    }
}
