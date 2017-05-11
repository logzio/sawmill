package io.logz.sawmill;

import io.logz.sawmill.conditions.AndCondition;
import io.logz.sawmill.conditions.TestCondition;
import io.logz.sawmill.parser.ConditionDefinition;
import io.logz.sawmill.parser.ConditionalExecutionStepDefinition;
import io.logz.sawmill.parser.ExecutionStepDefinition;
import io.logz.sawmill.parser.ExecutionStepsParser;
import io.logz.sawmill.parser.ProcessorDefinition;
import io.logz.sawmill.parser.ProcessorExecutionStepDefinition;
import io.logz.sawmill.processors.AddTagProcessor;
import io.logz.sawmill.processors.TestProcessor;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.logz.sawmill.utilities.JsonUtils.createList;
import static io.logz.sawmill.utilities.JsonUtils.createMap;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ExecutionStepsParserTest {

    private ExecutionStepsParser executionStepsParser;

    @Before
    public void init() {
        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        processorFactoryRegistry.register("test", new TestProcessor.Factory());
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcessors(processorFactoryRegistry);

        ConditionFactoryRegistry conditionFactoryRegistry = new ConditionFactoryRegistry();
        conditionFactoryRegistry.register("testCondition", new TestCondition.Factory());
        ConditionalFactoriesLoader.getInstance().loadAnnotatedConditions(conditionFactoryRegistry);

        executionStepsParser = new ExecutionStepsParser(processorFactoryRegistry, conditionFactoryRegistry);
    }

    @Test
    public void testParseProcessorExecutionStep() {
        List<ExecutionStepDefinition> executionStepDefinitionList = Collections.singletonList(
                createAddTagStepDefinition()
        );

        List<ExecutionStep> executionSteps = executionStepsParser.parse(executionStepDefinitionList);
        assertThat(executionSteps.size()).isEqualTo(1);

        ProcessorExecutionStep processorExecutionStep = (ProcessorExecutionStep) executionSteps.get(0);
        assertThat(processorExecutionStep.getProcessor()).isInstanceOf(AddTagProcessor.class);

        Optional<List<ExecutionStep>> onFailureExecutionSteps = processorExecutionStep.getOnFailureExecutionSteps();
        assertThat(onFailureExecutionSteps.isPresent()).isFalse();
    }

    @Test
    public void testParseOnFailureExecutionStep() {
        List<ExecutionStepDefinition> executionStepDefinitionList = Collections.singletonList(
                createAddTagStepDefinition(Collections.singletonList(
                    createAddTagStepDefinition()
                ))
        );

        List<ExecutionStep> executionSteps = executionStepsParser.parse(executionStepDefinitionList);

        ProcessorExecutionStep processorExecutionStep = (ProcessorExecutionStep) executionSteps.get(0);
        List<ExecutionStep> onFailureExecutionSteps = processorExecutionStep.getOnFailureExecutionSteps().get();

        assertThat(onFailureExecutionSteps.size()).isEqualTo(1);

        ProcessorExecutionStep onFailureExecutionStep = (ProcessorExecutionStep) onFailureExecutionSteps.get(0);
        assertThat(onFailureExecutionStep.getProcessor()).isInstanceOf(AddTagProcessor.class);
    }

    @Test
    public void testParseOnSuccessExecutionStep() {
        List<ExecutionStepDefinition> executionStepDefinitionList = Collections.singletonList(
                createAddTagStepDefinition(null, null, Collections.singletonList(createAddTagStepDefinition()))
        );

        List<ExecutionStep> executionSteps = executionStepsParser.parse(executionStepDefinitionList);

        ProcessorExecutionStep processorExecutionStep = (ProcessorExecutionStep) executionSteps.get(0);
        List<ExecutionStep> onSuccessExecutionSteps = processorExecutionStep.getOnSuccessExecutionSteps().get();

        assertThat(onSuccessExecutionSteps.size()).isEqualTo(1);

        ProcessorExecutionStep onSuccessExecutionStep = (ProcessorExecutionStep) onSuccessExecutionSteps.get(0);
        assertThat(onSuccessExecutionStep.getProcessor()).isInstanceOf(AddTagProcessor.class);
    }

    @Test
    public void testParseConditionalExecutionStep() {
        List<ExecutionStepDefinition> executionStepDefinitionList = Collections.singletonList(
                new ConditionalExecutionStepDefinition(
                        createAndExistsConditionDefinition(),
                        Collections.singletonList(
                                createAddTagStepDefinition()
                        ),
                        Collections.singletonList(
                                createAddTagStepDefinition()
                        )));

        List<ExecutionStep> executionSteps = executionStepsParser.parse(executionStepDefinitionList);
        assertThat(executionSteps.size()).isEqualTo(1);

        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) executionSteps.get(0);

        assertThat(conditionalExecutionStep.getCondition()).isInstanceOf(AndCondition.class);

        ProcessorExecutionStep onTrueExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnTrue().get(0);
        assertThat(onTrueExecutionStep.getProcessor()).isInstanceOf(AddTagProcessor.class);

        ProcessorExecutionStep onFalseExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnFalse().get(0);
        assertThat(onFalseExecutionStep.getProcessor()).isInstanceOf(AddTagProcessor.class);
    }

    @Test
    public void testDefaultProcessorName() {
        List<ExecutionStepDefinition> executionStepDefinitionList = Arrays.asList(
                new ConditionalExecutionStepDefinition(
                        createAndExistsConditionDefinition(),
                        Collections.singletonList(
                                createAddTagStepDefinition()
                        ),
                        Collections.singletonList(
                                createAddTagStepDefinition()
                        )),
                createAddTagStepDefinition(Collections.singletonList(
                        createAddTagStepDefinition()
                ))
        );

        List<ExecutionStep> executionSteps = executionStepsParser.parse(executionStepDefinitionList);
        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) executionSteps.get(0);

        ProcessorExecutionStep onTrueExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnTrue().get(0);
        assertThat(onTrueExecutionStep.getProcessorName()).isEqualTo("[addTag1]");

        ProcessorExecutionStep onFalseExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnFalse().get(0);
        assertThat(onFalseExecutionStep.getProcessorName()).isEqualTo("[addTag2]");

        ProcessorExecutionStep processorExecutionStep = (ProcessorExecutionStep) executionSteps.get(1);
        assertThat(processorExecutionStep.getProcessorName()).isEqualTo("[addTag3]");

        ProcessorExecutionStep onFailureExecutionStep = (ProcessorExecutionStep) processorExecutionStep.getOnFailureExecutionSteps().get().get(0);
        assertThat(onFailureExecutionStep.getProcessorName()).isEqualTo("[addTag4]");
    }

    @Test
    public void testProcessorName() {
        String processorName1 = RandomStringUtils.randomAlphanumeric(10);
        String processorName2 = RandomStringUtils.randomAlphanumeric(10);
        String processorName3 = RandomStringUtils.randomAlphanumeric(10);
        String processorName4 = RandomStringUtils.randomAlphanumeric(10);

        List<ExecutionStepDefinition> executionStepDefinitionList = Arrays.asList(
                new ConditionalExecutionStepDefinition(
                        createAndExistsConditionDefinition(),
                        Collections.singletonList(
                                createAddTagStepDefinition(processorName1)
                        ),
                        Collections.singletonList(
                                createAddTagStepDefinition(processorName2)
                        )),
                createAddTagStepDefinition(processorName3, Collections.singletonList(
                        createAddTagStepDefinition(processorName4)
                ))
        );

        List<ExecutionStep> executionSteps = executionStepsParser.parse(executionStepDefinitionList);
        ConditionalExecutionStep conditionalExecutionStep = (ConditionalExecutionStep) executionSteps.get(0);

        ProcessorExecutionStep onTrueExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnTrue().get(0);
        assertThat(onTrueExecutionStep.getProcessorName()).isEqualTo("[addTag1]" + processorName1);

        ProcessorExecutionStep onFalseExecutionStep = (ProcessorExecutionStep) conditionalExecutionStep.getOnFalse().get(0);
        assertThat(onFalseExecutionStep.getProcessorName()).isEqualTo("[addTag2]" + processorName2);

        ProcessorExecutionStep processorExecutionStep = (ProcessorExecutionStep) executionSteps.get(1);
        assertThat(processorExecutionStep.getProcessorName()).isEqualTo("[addTag3]" + processorName3);

        ProcessorExecutionStep onFailureExecutionStep = (ProcessorExecutionStep) processorExecutionStep.getOnFailureExecutionSteps().get().get(0);
        assertThat(onFailureExecutionStep.getProcessorName()).isEqualTo("[addTag4]" + processorName4);
    }

    private ConditionDefinition createAndExistsConditionDefinition() {
        return new ConditionDefinition("and", createMap("conditions", createList(
                new ConditionDefinition("exists", createMap(
                        "field", "field1"
                )),
                new ConditionDefinition("exists", createMap(
                        "field", "field2"
                ))
        )));
    }

    private ProcessorExecutionStepDefinition createAddTagStepDefinition() {
        return createAddTagStepDefinition(null, null);
    }

    private ProcessorExecutionStepDefinition createAddTagStepDefinition(String name) {
        return createAddTagStepDefinition(name, null);
    }

    private ProcessorExecutionStepDefinition createAddTagStepDefinition(List<ExecutionStepDefinition> onFailureExecutionStepDefinitions) {
        return createAddTagStepDefinition(null, onFailureExecutionStepDefinitions);
    }

    private ProcessorExecutionStepDefinition createAddTagStepDefinition(String name, List<ExecutionStepDefinition> onFailureExecutionStepDefinitions) {
        return createAddTagStepDefinition(name, onFailureExecutionStepDefinitions, null);
    }

    private ProcessorExecutionStepDefinition createAddTagStepDefinition(String name,
                                                                        List<ExecutionStepDefinition> onFailureExecutionStepDefinitions,
                                                                        List<ExecutionStepDefinition> onSuccessExecutionStepDefinitions) {
        return new ProcessorExecutionStepDefinition(createAddTagProcessorDefinition(), name,
                onFailureExecutionStepDefinitions, onSuccessExecutionStepDefinitions);
    }

    private ProcessorDefinition createAddTagProcessorDefinition() {
        return new ProcessorDefinition("addTag", createMap("tags", createList("tag1", "tag2")));
    }

}