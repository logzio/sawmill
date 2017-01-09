package io.logz.sawmill.parser;

import io.logz.sawmill.Condition;
import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.ConditionalExecutionStep;
import io.logz.sawmill.ExecutionStep;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorExecutionStep;
import io.logz.sawmill.ProcessorFactoryRegistry;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExecutionStepsParser {
    private final ProcessorParser processorParser;
    private final ConditionParser conditionParser;

    public ExecutionStepsParser(ProcessorFactoryRegistry processorFactoryRegistry, ConditionFactoryRegistry conditionFactoryRegistry) {
        this.processorParser = new ProcessorParser(processorFactoryRegistry);
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
        Processor processor = processorParser.parse(processorExecutionStepDefinition.getProcessorDefinition());
        String processorName = processorExecutionStepDefinition.getName();
        Optional<List<ExecutionStepDefinition>> onFailureExecutionStepDefinitions =
                processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList();
        List<ExecutionStep> executionSteps = onFailureExecutionStepDefinitions.isPresent() ? parse(onFailureExecutionStepDefinitions.get()) : null;

        return new ProcessorExecutionStep(processorName, processor, executionSteps);
    }

}
