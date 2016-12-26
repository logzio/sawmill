package io.logz.sawmill.parser;

import io.logz.sawmill.Condition;
import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.ConditionalExecutionStep;
import io.logz.sawmill.ExecutionStep;
import io.logz.sawmill.OnFailureExecutionStep;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorExecutionStep;
import io.logz.sawmill.ProcessorFactoryRegistry;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
        Condition parsedCondition = conditionParser.parse(conditionalExecutionStepDefinition.getConditionDefinition());
        List<ExecutionStep> parsedOnTrue = parse(conditionalExecutionStepDefinition.getOnTrue());

        Optional<List<ExecutionStepDefinition>> optionalOnFalse = conditionalExecutionStepDefinition.getOnFalse();
        List<ExecutionStep> parsedOnFalse = optionalOnFalse.isPresent() ? parse(optionalOnFalse.get()) : Collections.emptyList();

        return new ConditionalExecutionStep(parsedCondition, parsedOnTrue, parsedOnFalse);
    }

    private ProcessorExecutionStep parseProcessorExecutionStep(ProcessorExecutionStepDefinition processorExecutionStepDefinition) {
        Processor processor = parseProcessor(processorExecutionStepDefinition.getProcessorDefinition());
        String processorName = processorExecutionStepDefinition.getName();
        List<OnFailureExecutionStep> onFailureExecutionSteps =
                parseOnFailureExecutionSteps(processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList());

        return new ProcessorExecutionStep(processorName, processor, onFailureExecutionSteps);
    }

    private Processor parseProcessor(ProcessorDefinition processorDefinition) {
        Processor.Factory factory = processorFactoryRegistry.get(processorDefinition.getType());
        return factory.create(processorDefinition.getConfig());
    }

    private List<OnFailureExecutionStep> parseOnFailureExecutionSteps(Optional<List<OnFailureExecutionStepDefinition>> onFailureExecutionStepDefinitionList) {
        if (!onFailureExecutionStepDefinitionList.isPresent()) return null;

        List<OnFailureExecutionStep> onFailureExecutionSteps =
                onFailureExecutionStepDefinitionList.get().stream().map(this::parseOnFailureExecutionStep).collect(Collectors.toList());
        return onFailureExecutionSteps;

    }

    private OnFailureExecutionStep parseOnFailureExecutionStep(OnFailureExecutionStepDefinition onFailureExecutionStepDefinition) {
        String name = onFailureExecutionStepDefinition.getName();
        Processor processor = parseProcessor(onFailureExecutionStepDefinition.getProcessorDefinition());
        return new OnFailureExecutionStep(name, processor);
    }
}
