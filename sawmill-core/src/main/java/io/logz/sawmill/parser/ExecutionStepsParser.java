package io.logz.sawmill.parser;

import io.logz.sawmill.Condition;
import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.ConditionalExecutionStep;
import io.logz.sawmill.ExecutionStep;
import io.logz.sawmill.OnFailureExecutionStep;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorExecutionStep;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.parser.ConditionalExecutionStepDefinition;
import io.logz.sawmill.parser.ExecutionStepDefinition;
import io.logz.sawmill.parser.OnFailureExecutionStepDefinition;
import io.logz.sawmill.parser.ProcessorDefinition;
import io.logz.sawmill.parser.ProcessorExecutionStepDefinition;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by naorguetta on 22/12/2016.
 */
public class ExecutionStepsParser {
    private final ProcessorFactoryRegistry processorFactoryRegistry;
    private final ConditionParser conditionParser;

    public ExecutionStepsParser(ProcessorFactoryRegistry processorFactoryRegistry, ConditionFactoryRegistry conditionFactoryRegistry) {
        this.processorFactoryRegistry = processorFactoryRegistry;
        this.conditionParser = new ConditionParser(conditionFactoryRegistry);
    }

    public List<ExecutionStep> parse(List<ExecutionStepDefinition> executionStepDefinitionList) {
        return executionStepDefinitionList.stream().map(this::parse).collect(Collectors.toList());
    }

    private ExecutionStep parse(ExecutionStepDefinition executionStepDefinition) {
        if (executionStepDefinition instanceof ConditionalExecutionStepDefinition) {
            ConditionalExecutionStepDefinition conditionalExecutionStep = (ConditionalExecutionStepDefinition) executionStepDefinition;
            return parseConditionalExecutionStep(conditionalExecutionStep);
        }

        ProcessorExecutionStepDefinition processorExecutionStep = (ProcessorExecutionStepDefinition) executionStepDefinition;
        return parseProcessorExecutionStep(processorExecutionStep);
    }

    private ConditionalExecutionStep parseConditionalExecutionStep(ConditionalExecutionStepDefinition conditionalExecutionStepDefinition) {
        Condition condition = conditionParser.parse(conditionalExecutionStepDefinition.getCondition());
        List<ExecutionStep> onTrueExecutionSteps = parse(conditionalExecutionStepDefinition.getOnTrue());
        List<ExecutionStep> onFalseExecutionSteps = parse(conditionalExecutionStepDefinition.getOnFalse());

        return new ConditionalExecutionStep(condition, onTrueExecutionSteps, onFalseExecutionSteps);
    }

    private ProcessorExecutionStep parseProcessorExecutionStep(ProcessorExecutionStepDefinition processorExecutionStepDefinition) {
        Processor processor = parseProcessor(processorExecutionStepDefinition.getProcessorDefinition());
        String processorName = processorExecutionStepDefinition.getName();
        List<OnFailureExecutionStep> onFailureExecutionSteps = parseOnFailureExecutionSteps(processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList());
        return new ProcessorExecutionStep(processorName, processor, onFailureExecutionSteps);
    }

    private Processor parseProcessor(ProcessorDefinition processorDefinition) {
        Processor.Factory factory = processorFactoryRegistry.get(processorDefinition.getType());
        return factory.create(processorDefinition.getConfig());
    }

    private List<OnFailureExecutionStep> parseOnFailureExecutionSteps(List<OnFailureExecutionStepDefinition> onFailureExecutionStepDefinitionList) {
        if (onFailureExecutionStepDefinitionList == null) return null;
        return onFailureExecutionStepDefinitionList.stream().map(this::parseOnFailureExecutionStep).collect(Collectors.toList());

    }

    private OnFailureExecutionStep parseOnFailureExecutionStep(OnFailureExecutionStepDefinition onFailureExecutionStepDefinition) {
        String name = onFailureExecutionStepDefinition.getName();
        Processor processor = parseProcessor(onFailureExecutionStepDefinition.getProcessorDefinition());
        return new OnFailureExecutionStep(name, processor);
    }
}
