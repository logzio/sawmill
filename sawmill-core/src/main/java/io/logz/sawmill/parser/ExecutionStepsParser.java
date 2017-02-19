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
        return parse(executionStepDefinitionList, new IdGenerator());
    }

    private List<ExecutionStep> parse(List<ExecutionStepDefinition> executionStepDefinitionList, IdGenerator idGenerator) {
        return executionStepDefinitionList.stream().map(stepDefinition -> parse(stepDefinition, idGenerator)).collect(Collectors.toList());
    }

    private ExecutionStep parse(ExecutionStepDefinition executionStepDefinition, IdGenerator idGenerator) {
        if (executionStepDefinition instanceof ConditionalExecutionStepDefinition) {
            ConditionalExecutionStepDefinition conditionalExecutionStep = (ConditionalExecutionStepDefinition) executionStepDefinition;
            return parseConditionalExecutionStep(conditionalExecutionStep, idGenerator);
        } else if (executionStepDefinition instanceof ProcessorExecutionStepDefinition) {
            ProcessorExecutionStepDefinition processorExecutionStep = (ProcessorExecutionStepDefinition) executionStepDefinition;
            return parseProcessorExecutionStep(processorExecutionStep, idGenerator);
        }

        throw new RuntimeException("Unsupported execution step definition: " + executionStepDefinition.getClass());
    }

    private ConditionalExecutionStep parseConditionalExecutionStep(ConditionalExecutionStepDefinition conditionalExecutionStepDefinition, IdGenerator idGenerator) {
        Condition parsedCondition = conditionParser.parse(conditionalExecutionStepDefinition.getConditionDefinition());

        List<ExecutionStep> parsedOnTrue = parse(conditionalExecutionStepDefinition.getOnTrue(), idGenerator);

        Optional<List<ExecutionStepDefinition>> optionalOnFalse = conditionalExecutionStepDefinition.getOnFalse();
        List<ExecutionStep> parsedOnFalse = optionalOnFalse.isPresent() ? parse(optionalOnFalse.get(), idGenerator) : Collections.emptyList();

        return new ConditionalExecutionStep(parsedCondition, parsedOnTrue, parsedOnFalse);
    }

    private ProcessorExecutionStep parseProcessorExecutionStep(ProcessorExecutionStepDefinition processorExecutionStepDefinition, IdGenerator idGenerator) {
        String processorId = getProcessorName(processorExecutionStepDefinition, idGenerator);
        Processor processor = processorParser.parse(processorExecutionStepDefinition.getProcessorDefinition());
        Optional<List<ExecutionStepDefinition>> onFailureExecutionStepDefinitions =
                processorExecutionStepDefinition.getOnFailureExecutionStepDefinitionList();
        List<ExecutionStep> executionSteps = onFailureExecutionStepDefinitions.isPresent() ? parse(onFailureExecutionStepDefinitions.get(), idGenerator) : null;

        return new ProcessorExecutionStep(processorId, processor, executionSteps);
    }

    private String getProcessorName(ProcessorExecutionStepDefinition processorExecutionStepDefinition, IdGenerator idGenerator) {
        String type = processorExecutionStepDefinition.getProcessorDefinition().getType();
        String prefix = "[" + type + idGenerator.getNextId() + "]";
        Optional<String> optionalName = processorExecutionStepDefinition.getName();

        return prefix + optionalName.orElse("");
    }

    private class IdGenerator {
        private int id;

        public IdGenerator() {
            id = 1;
        }

        public int getNextId() {
            return id++;
        }
    }

}
